package scheduling

import (
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const edgeUrl = "edge"
const profilingParamsJson = "examples/profiling_params.json"

var edgeEnabled = false
var cloudEnabled = false
var lambdaEnabled = false

type IlpOffloadingPolicy struct {
	probabilityCache sync.Map
	updateInterval   time.Duration
	httpClient       *http.Client

	totalArrivals      map[string]int64
	totalArrivalsMutex sync.RWMutex
	policyStartTime    time.Time
	arrivalRates       map[string]float64 // Mappa dei tassi di arrivo "smussati" (req/sec)
	arrivalRatesMutex  sync.RWMutex       // RWMutex per proteggere arrivalRates
	arrivalAlpha       float64            // Fattore di smoothing per la Media Mobile Esponenziale (EMA)

	actualBudgetUsed      float64
	actualBudgetUsedMutex sync.RWMutex

	profilingThreshold int // Profiling Threshold
}

func (policy *IlpOffloadingPolicy) Init() {
	edgeEnabled = config.GetBool(config.EDGE_NODES_ENABLED, false)
	cloudEnabled = config.GetBool(config.CLOUD_NODES_ENABLED, false)
	lambdaEnabled = config.GetBool(config.EXTERNAL_PROVIDER_ENABLED, false)
	log.Printf("Cloud enabled: %v\n", cloudEnabled)
	log.Printf("Edge enabled: %v\n", edgeEnabled)
	log.Printf("Lambda enabled: %v\n", lambdaEnabled)
	updateIntervalSeconds := config.GetInt(config.FUNCTION_OFFLOADING_POLICY_LAMBDA_PING_INTERVAL, 15)
	policy.updateInterval = time.Duration(updateIntervalSeconds) * time.Second
	policy.httpClient = &http.Client{Timeout: 30 * time.Second}

	//Taking Qos Classes
	qosPath := config.GetString(config.FUNCTION_OFFLOADING_QOS_CLASSES_PATH, "")

	err := loadQoSDefinitions(qosPath)
	if err != nil {
		log.Printf("Impossible to load QoS classes definitions: %v", err)
		return
	}

	alpha := config.GetFloat(config.POLICY_ARRIVAL_RATE_ALPHA, 0.3)
	policy.arrivalAlpha = alpha
	policy.totalArrivals = make(map[string]int64)
	policy.arrivalRates = make(map[string]float64)
	policy.policyStartTime = time.Now()

	//Profiling minimum number
	minExecutionNumber := config.GetInt(
		config.FUNCTION_OFFLOADING_POLICY_PROFILING_MIN_EXEC_NUMBER,
		5,
	)

	numberProfilingExecution := config.GetInt(
		config.FUNCTION_OFFLOADING_POLICY_PROFILING_TRIGGER,
		5,
	)
	policy.profilingThreshold = numberProfilingExecution
	log.Printf("Loading profiling params")
	err = LoadProfilingParams(profilingParamsJson)
	if err != nil {
		log.Printf("Impossible to load profiling params: %v", err)
		return
	}

	//Lambda RTT Monitor
	if lambdaEnabled {
		fnPingFunction := config.GetString(config.POLICY_FUNCTION_NAME, "")
		lambda.InitRttMonitor(policy.updateInterval, fnPingFunction)
	}

	log.Println("Starting policy polling process...")
	go policy.optimizerLoop(minExecutionNumber, edgeEnabled, cloudEnabled, lambdaEnabled)
}

func (policy *IlpOffloadingPolicy) OnArrival(r *scheduledRequest) {
	qosName, ok := getQoSClassNameByID(r.Class)
	if !ok {
		log.Printf("QoS class name not registered, plese add it, error: %v", r.Class)
	}

	key := r.Request.Fun.Name + "|" + qosName
	policy.totalArrivalsMutex.Lock()
	policy.totalArrivals[key]++
	policy.totalArrivalsMutex.Unlock()

	decision, err := policy.evaluate(r, key)
	if err != nil {
		log.Printf("Error calling Evaluate request: %v. Dropping it...", err)
		dropRequest(r)
	}

	var actionChoice string
	if decision.action == 0 {
		actionChoice = "Drop"
	} else if decision.action == 1 {
		actionChoice = "Execute locally"
	} else if decision.action == 2 {
		actionChoice = "Execute remotely on " + decision.remoteHost
	}

	log.Printf("Action choiced by evaluator: %s\n", actionChoice)

	if decision.action == 0 {
		dropRequest(r)
	} else if decision.action == 1 { //Local execution
		containerID, warm, err := node.AcquireContainer(r.Fun, false)
		if err == nil {
			execLocally(r, containerID, warm)
		} else {
			log.Printf("Error in choosing container: %v", err)
		}
	} else if decision.action == 2 && decision.remoteHost == edgeUrl { //Offload on Edge node
		selectedEdge := pickEdgeNodeForOffloading(r)
		if selectedEdge == "" { //Here we can send to Lambda or Drop, for now we drop
			log.Printf("No edge peer available, dropping request...")
			dropRequest(r)
			return
		}

		handleOffload(r, selectedEdge)

	} else if decision.action == 2 {
		if lambdaEnabled {
			handleLambdaOffload(r)
		} else {
			handleCloudOffload(r)
		}
	}

	log.Printf("Execution of function: %s  with action: %s done\n.", r.Fun.Name, actionChoice)
}

func (policy *IlpOffloadingPolicy) OnCompletion(fun *function.Function, executionReport *function.ExecutionReport) {
}

func (policy *IlpOffloadingPolicy) evaluate(r *scheduledRequest, cacheKey string) (schedDecision, error) {
	value, ok := policy.probabilityCache.Load(cacheKey)
	var currentProbs Probs
	if ok {
		currentProbs = value.(Probs)
	} else {
		currentProbs = Probs{
			PLocal: 0.4,
			PCloud: 0.3,
			PEdge:  0.3,
			PDrop:  0.0,
		}
	}

	if !r.CanDoOffloading {
		currentProbs.PCloud = 0
		currentProbs.PEdge = 0

		if !node.CanExecuteLocally(r.Fun.CPUDemand, r.Fun.MemoryMB) {
			currentProbs.PLocal = 0
		}

		sum := currentProbs.PLocal + currentProbs.PDrop
		if sum == 0 {
			return schedDecision{action: DROP}, nil
		}

		currentProbs.PLocal = currentProbs.PLocal / sum
		currentProbs.PDrop = currentProbs.PDrop / sum
		return randomizedChoice(currentProbs)
	}

	if !node.CanExecuteLocally(r.Fun.CPUDemand, r.Fun.MemoryMB) {
		currentProbs.PLocal = 0
	}

	if !cloudEnabled {
		currentProbs.PCloud = 0
	}

	if !edgeEnabled {
		currentProbs.PEdge = 0
	}

	sum := currentProbs.PLocal + currentProbs.PCloud + currentProbs.PEdge + currentProbs.PDrop

	if sum == 0 {
		return schedDecision{action: DROP}, nil
	}

	if sum != 1.0 {
		currentProbs.PLocal = currentProbs.PLocal / sum
		currentProbs.PCloud = currentProbs.PCloud / sum
		currentProbs.PEdge = currentProbs.PEdge / sum
		currentProbs.PDrop = currentProbs.PDrop / sum
	}
	return randomizedChoice(currentProbs)
}

func randomizedChoice(probs Probs) (schedDecision, error) {
	if probs.PEdge+probs.PCloud+probs.PLocal <= 0 {
		return schedDecision{action: DROP}, nil
	}

	sum := probs.PLocal + probs.PCloud + probs.PEdge + probs.PDrop

	pLocal := probs.PLocal / sum
	pCloud := probs.PCloud / sum
	pEdge := probs.PEdge / sum

	randValue := rand.Float64()

	if randValue < pLocal {
		return schedDecision{action: EXEC_LOCAL}, nil
	} else if randValue < pLocal+pCloud {
		return schedDecision{action: EXEC_REMOTE}, nil
	} else if randValue < pLocal+pCloud+pEdge {
		return schedDecision{action: EXEC_REMOTE, remoteHost: edgeUrl}, nil
	} else {
		return schedDecision{action: DROP}, nil
	}
}
