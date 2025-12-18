package scheduling

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/externalprovider"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda/utils"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	send_utils "github.com/serverledge-faas/serverledge/utils"
	"golang.org/x/exp/slices"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"
)

const LOCAL = 0
const EDGE = 1
const LAMBDA = 2
const CLOUD = 3

var profilingParams map[string]map[string]interface{}

type Probs struct {
	PLocal float64 `json:"PLocal"`
	PCloud float64 `json:"PCloud"`
	PEdge  float64 `json:"PEdge"`
	PDrop  float64 `json:"PDrop"`
}

type optimizerPayload struct {
	Functions []FunctionDef `json:"functions"`
	Classes   []QoSClass    `json:"classes"`

	Budget                float64 `json:"budget"`
	CloudComputeCostGbSec float64 `json:"cloud_compute_cost_gbsec"`
	CloudRequestCost      float64 `json:"cloud_request_cost"`
	GenericCloudCost      float64 `json:"cloud_cost"`

	HandlingNode         string             `json:"handling_node"`
	EdgeNodes            []string           `json:"edge_nodes"`
	CloudNodes           []string           `json:"cloud_nodes"`
	NodeMemory           map[string]float64 `json:"node_memory"`
	NodeLatency          map[string]float64 `json:"node_latency"`
	BandwidthEdge        float64            `json:"bandwidth_edge"`
	BandwidthCloud       float64            `json:"bandwidth_cloud"`
	ArrivalRates         map[string]float64 `json:"arrival_rates"`
	ExecTime             map[string]float64 `json:"exec_time"`
	InitTime             map[string]float64 `json:"init_time"`
	AggregatedEdgeMemory float64            `json:"aggregated_edge_memory"`
}

type FunctionDef struct {
	Name      string `json:"name"`
	MemoryMB  int64  `json:"memory"`
	InputSize int64  `json:"input_size"`
}

type QoSClass struct {
	Id              int64   `yaml:"id" json:"id"`
	Name            string  `yaml:"name" json:"name"`
	MaxRespTime     float64 `yaml:"max_resp_time" json:"max_resp_time"`
	Utility         float64 `yaml:"utility" json:"utility"`
	DeadlinePenalty float64 `yaml:"deadline_penalty" json:"deadline_penalty"`
	DropPenalty     float64 `yaml:"drop_penalty" json:"drop_penalty"`
}

func initOptimizerParams() optimizerPayload {
	return optimizerPayload{
		NodeMemory:   make(map[string]float64),
		NodeLatency:  make(map[string]float64),
		ArrivalRates: make(map[string]float64),
		ExecTime:     make(map[string]float64),
		InitTime:     make(map[string]float64),
	}
}

var qosRegistry = make(map[int64]QoSClass)

func (policy *IlpOffloadingPolicy) optimizerLoop(profilingMinExecution int, edgeEnabled bool, cloudEnabled bool, externalProviderEnabled bool) {
	optimizerUpdate := config.GetInt(config.OPTIMIZER_WAITING_INTERVAL, 30)
	ticker := time.NewTicker(time.Duration(optimizerUpdate) * time.Second)
	defer ticker.Stop()

	profilingInterval := config.GetInt(config.PROFILING_INTERVAL, 120)

	profilingTicker := time.NewTicker(time.Duration(profilingInterval) * time.Second)
	defer profilingTicker.Stop()

	lastProfilingCheck := time.Now()

	for {
		select {
		case <-ticker.C:
			log.Println("Polling: Begin strategic update...")

			policy.calculateArrivalRates()

			params, err := policy.prepareOptimizerParams()

			if err != nil {
				log.Printf("Error in the preparation of parameters, skipping optimization...: %v", err)
				continue
			}

			PrintOptimizerPayload(params)

			jsonData, err := json.Marshal(params)
			if err != nil {
				log.Printf("Polling: Error in marshalling: %v", err)
				continue
			}

			ilpOptimizerHost := config.GetString(config.FUNCTION_OFFLOADING_POLICY_OPTIMIZER_HOST, "localhost")
			ilpOptimizerPort := config.GetInt(config.FUNCTION_OFFLOADING_POLICY_OPTIMIZER_PORT, 8080)
			url := fmt.Sprintf("http://%s:%d/optimize", ilpOptimizerHost, ilpOptimizerPort)

			resp, err := policy.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Polling: Errore nella chiamata all'ottimizzatore: %v", err)
				continue
			}

			if resp.StatusCode == http.StatusOK {
				var newProbs map[string]Probs
				if err := json.NewDecoder(resp.Body).Decode(&newProbs); err == nil {
					for key, value := range newProbs {
						policy.probabilityCache.Store(key, value)
					}
					log.Printf("Polling: Cache of probability update done")
				} else {
					log.Printf("Polling: Error in decoding the response: %v", err)
				}
			} else {
				log.Printf("Polling: Response status: %s", resp.Status)
			}
			if closeErr := resp.Body.Close(); closeErr != nil {
				log.Printf("Errorin closing body for %s: %v", url, closeErr)
			}
		case now := <-profilingTicker.C:
			intervalSeconds := now.Sub(lastProfilingCheck).Seconds()
			lastProfilingCheck = now
			log.Printf("Polling: Start profiling...")
			go policy.performStrategicUpdate(profilingMinExecution, int(intervalSeconds), edgeEnabled, cloudEnabled, externalProviderEnabled)
		}
	}
}

func (policy *IlpOffloadingPolicy) performStrategicUpdate(profilingMinExecution int, intervalSeconds int, edgeEnabled bool, cloudEnabled bool, externalProviderEnabled bool) {
	profilingMap := policy.needProfiling(profilingMinExecution, intervalSeconds)

	for fName, profiling := range profilingMap {
		if profiling[LOCAL] {
			policy.triggerProfilingRun(fName, LOCAL, 5)
		}
		if profiling[EDGE] && edgeEnabled {
			policy.triggerProfilingRun(fName, EDGE, 5)
		}
		if profiling[CLOUD] && cloudEnabled {
			policy.triggerProfilingRun(fName, CLOUD, 5)
		}
		if profiling[LAMBDA] && externalProviderEnabled {
			policy.triggerProfilingRun(fName, LAMBDA, 5)
		}
	}
}

func LoadProfilingParams(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read profiling params file: %w", err)
	}

	err = json.Unmarshal(data, &profilingParams)
	if err != nil {
		return fmt.Errorf("failed to unmarshal profiling params: %w", err)
	}

	log.Println("Profiling parameters loaded successfully.")
	return nil
}

func getProfilingParams(funcName string) (map[string]interface{}, bool) {
	params, found := profilingParams[funcName]
	return params, found
}

func (policy *IlpOffloadingPolicy) triggerProfilingRun(fName string, dest int, count int) {
	log.Printf("[Profiling] Starting %d executions of profiling for '%s' on %d", count, fName, dest)
	params, found := getProfilingParams(fName)
	if !found {
		log.Printf("Error: profiling parameters for '%s' not found.", fName)
		return
	}

	switch dest {
	case LOCAL:
		sendRequest(params, fName, count, LOCAL)
	case LAMBDA:
		sendLambdaRequest(params, fName, count)
	case EDGE:
		sendRequest(params, fName, count, EDGE)
	case CLOUD:
		sendRequest(params, fName, count, CLOUD)
	default:
		log.Println("Destination not valid")
	}
}

func sendLambdaRequest(params map[string]interface{}, fName string, count int) {
	request := client.InvocationRequest{
		Params:          params,
		CanDoOffloading: false,
	}

	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Printf("Error creating request... stopping process")
		return
	}
	log.Printf("Sending %d sequential requests to AWS Lambda for function '%s'...", count, fName)

	provider, err := lambda.GetProvider()
	if err != nil {
		log.Printf("Error getting Lambda provider: %v", err)
		return
	}

	fun, ok := function.GetFunction(fName)
	if !ok {
		log.Printf("Function '%s' not found", fName)
		return
	}

	for i := 0; i < count; i++ {
		log.Printf("Sending Lambda request %d of %d", i+1, count)

		sendingTime := time.Now()
		reqId := fmt.Sprintf("%s-profiling-%d-%d", fName, sendingTime.UnixNano(), i)
		ctx := context.WithValue(context.Background(), "ReqId", reqId)

		profilingRequest := &scheduledRequest{
			Request: &function.Request{
				Fun:             fun,
				Params:          params,
				Arrival:         sendingTime,
				CanDoOffloading: false,
				Async:           false,
				ReturnOutput:    true,
				Ctx:             ctx,
			},
			ExecutionReport: &function.ExecutionReport{},
			offloaded:       false,
			decisionChannel: nil,
		}

		report, err := provider.InvokeProviderFunction(profilingRequest.Request, invocationBody)

		if err != nil {
			fmt.Printf("Lambda invocation failed on request #%d: %v\n", i+1, err)
			// Notifica il fallimento a Prometheus
			completions <- &completionNotification{
				r:      profilingRequest,
				failed: true,
			}
			continue
		}

		now := time.Now()

		// Updating report
		profilingRequest.ExecutionReport = &report
		profilingRequest.ResponseTime = now.Sub(profilingRequest.Arrival).Seconds()
		coldStartTime := report.InitTime
		profilingRequest.BilledDuration = profilingRequest.Duration + coldStartTime
		profilingRequest.Duration = report.Duration
		profilingRequest.InitTime = coldStartTime + now.Sub(sendingTime).Seconds()
		profilingRequest.OffloadLatency = now.Sub(sendingTime).Seconds() -
			report.Duration
		profilingRequest.OffloadDestination = "awslambda"

		completions <- &completionNotification{
			r:      profilingRequest,
			failed: false,
			cont:   nil,
		}

		fmt.Printf("Request #%d completed - Duration: %.4fs, InitTime: %.4fs, WarmStart: %v\n",
			i+1, report.Duration, report.InitTime, report.IsWarmStart)
	}

	log.Printf("Successfully completed %d Lambda profiling requests.", count)
}

func sendRequest(params map[string]interface{}, fName string, count int, destination int) {
	request := client.InvocationRequest{
		Params:          params,
		CanDoOffloading: false,
	}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Printf("Error creating request.. stopping process")
		return
	}
	ip := ""
	port := 0
	switch destination {
	case LOCAL:
		ip = registration.SelfRegistration.IPAddress
		port = registration.SelfRegistration.APIPort
	case EDGE:
		nearestNodes := registration.GetNearestNeighbors() //We are considering only the nearest for now
		if len(nearestNodes) > 0 {
			closestEdgeNode := nearestNodes[0]
			ip = closestEdgeNode.IPAddress
			port = closestEdgeNode.APIPort
		} else {
			log.Printf("No edge node available")
			return
		}
	case CLOUD:
		cloudNode := registration.GetRemoteOffloadingTarget()
		if cloudNode != nil {
			ip = cloudNode.IPAddress
			port = cloudNode.APIPort
		} else {
			log.Printf("No cloud node available")
			return
		}
	default:
		log.Println("Destination not valid")
		return
	}

	url := fmt.Sprintf("http://%s:%d/invoke/%s", ip, port, fName)

	log.Printf("Sending %d sequential requests to %s...", count, url)

	for i := 0; i < count; i++ {
		log.Printf("Sending request %d of %d", i+1, count)
		resp, err := send_utils.PostJson(url, invocationBody)
		if err != nil {
			fmt.Printf("Invocation failed on request #%d: %v\n", i+1, err)
			return
		}

		send_utils.PrintJsonResponse(resp.Body)
		resp.Body.Close()
	}

	log.Printf("Successfully sent %d requests.", count)
}

func (policy *IlpOffloadingPolicy) prepareOptimizerParams() (optimizerPayload, error) {
	var LOCAL = registration.SelfRegistration.Key //local
	var EXTERNAL = "external"
	var CLOUD = "cloud"
	start := time.Now()
	params := initOptimizerParams()
	params.HandlingNode = LOCAL
	params.EdgeNodes = []string{LOCAL}
	params.NodeMemory[LOCAL] = (float64)(node.LocalResources.AvailableMemory())

	// Add available Edge peers
	nearbyServers := registration.GetFullNeighborInfo()

	if nearbyServers != nil {
		for k, v := range nearbyServers {
			availableCPU := v.TotalCPU - v.UsedCPU
			availableMemory := v.TotalMemory - v.UsedMemory
			if availableMemory > 0 && availableCPU > 0 {
				params.EdgeNodes = append(params.EdgeNodes, k)
				params.NodeMemory[k] = float64(availableMemory)
				params.AggregatedEdgeMemory += float64(availableMemory)
				// Cost (assuming that Edge nodes are all in the same area)
			}
		}
	}

	// Compute distances
	for key1, v1 := range nearbyServers {
		var distance float64
		if !slices.Contains(params.EdgeNodes, key1) {
			continue
		}
		distance = registration.VivaldiClient.DistanceTo(&v1.Coordinates).Seconds()
		params.NodeLatency[tupleKey(LOCAL, key1)] = distance
		params.NodeLatency[tupleKey(key1, LOCAL)] = distance

		for key2, v2 := range nearbyServers {
			if !slices.Contains(params.EdgeNodes, key2) {
				continue
			}
			if key1 == key2 {
				distance = 0.0
			} else {
				distance = v1.Coordinates.DistanceTo(&v2.Coordinates).Seconds()
			}
			params.NodeLatency[tupleKey(key1, key2)] = distance
			params.NodeLatency[tupleKey(key2, key1)] = distance
		}
	}

	edgeBandwidth := config.GetFloat( //Velocity in the LAN
		config.FUNCTION_OFFLOADING_POLICY_NODE_TO_EDGE,
		100.0, // default Mbps per edge
	)

	params.BandwidthEdge = edgeBandwidth

	isExternalProviderEnabled := config.GetBool(config.EXTERNAL_PROVIDER_ENABLED, false)

	params.Budget = config.GetFloat(config.FUNCTION_OFFLOADING_BUDGET, 0.0)

	costCloudGbSecMap := config.GetStringMapFloat64(config.FUNCTION_OFFLOADING_COMPUTE_REGION_COST_GB_SEC)
	costCloudReqMap := config.GetStringMapFloat64(config.FUNCTION_OFFLOADING_COMPUTE_REGION_REQUEST_COST)

	provider, err := externalprovider.NewFunctionOffloader(externalprovider.LambdaOffloader)
	if err != nil {
		return optimizerPayload{}, fmt.Errorf("impossible obtain provider: %w", err)
	}
	cloudRegion, err := provider.GetRegion()
	if err != nil {
		return optimizerPayload{}, fmt.Errorf("impossible obtain cloudRegion: %w", err)
	}

	cloudRegionKey := strings.ToLower(cloudRegion)
	cloudRegionGbSec, okGbSec := costCloudGbSecMap[cloudRegionKey]
	cloudRegionReq, okReq := costCloudReqMap[cloudRegionKey]

	if !okGbSec || !okReq {
		cloudRegionGbSec = 0.0
		cloudRegionReq = 0.0
	}

	if isExternalProviderEnabled {

		params.CloudNodes = []string{EXTERNAL}

		params.CloudComputeCostGbSec = cloudRegionGbSec
		params.CloudRequestCost = cloudRegionReq

		lambdaBw := config.GetFloat( //Bandwidth: Upload test
			config.FUNCTION_OFFLOADING_POLICY_LAMBDA_TO_CLOUD,
			100,
		)
		params.BandwidthCloud = lambdaBw

		//Now we need to measure latency to Lambda
		distanceToCloudDuration := provider.GetRtt()
		distanceToCloudSec := distanceToCloudDuration.Seconds()

		for _, n := range params.EdgeNodes {
			params.NodeLatency[tupleKey(n, EXTERNAL)] = distanceToCloudSec
			params.NodeLatency[tupleKey(EXTERNAL, n)] = distanceToCloudSec

		}
		params.NodeLatency[tupleKey(EXTERNAL, EXTERNAL)] = 0.0

	} else {

		remoteTarget := registration.GetRemoteOffloadingTarget()
		if remoteTarget == nil {
			//There isn't Cloud Node
		} else {
			params.CloudNodes = append(params.CloudNodes, CLOUD)

			params.CloudComputeCostGbSec = cloudRegionGbSec
			params.CloudRequestCost = cloudRegionReq

			params.BandwidthCloud = config.GetFloat(config.FUNCTION_OFFLOADING_CLOUD_BANDWIDTH, 100.0)

			distanceToCloud := registration.GetRemoteOffloadingTargetLatencyMs() / 1000.0

			for _, n := range params.EdgeNodes {
				params.NodeLatency[tupleKey(n, CLOUD)] = distanceToCloud
				params.NodeLatency[tupleKey(CLOUD, n)] = distanceToCloud
			}
			params.NodeLatency[tupleKey(CLOUD, CLOUD)] = 0.0
		}
	}

	//QoS
	allClasses := getAllQoSClasses()
	params.Classes = make([]QoSClass, 0, len(allClasses))

	for _, metadata := range allClasses {
		newQos := QoSClass{
			Id:              metadata.Id,
			Name:            metadata.Name,
			MaxRespTime:     metadata.MaxRespTime,
			Utility:         metadata.Utility,
			DeadlinePenalty: metadata.DeadlinePenalty,
			DropPenalty:     metadata.DropPenalty,
		}
		params.Classes = append(params.Classes, newQos)
	}

	functionNames, err := function.GetAll()

	if err != nil {
		return optimizerPayload{}, fmt.Errorf("impossible obtain functionNames: %w", err)
	}

	retrievedMetrics := metrics.GetMetrics()

	for _, fnName := range functionNames {
		realFunc, ok := function.GetFunction(fnName)
		if !ok {
			log.Printf("Impossible get the function %s, skipping...", fnName)
			continue
		}

		var avgInputSize = 1024.0
		if size, ok := retrievedMetrics.AvgInputSize[fnName]; ok && size > 0 {
			avgInputSize = size
		}
		newFuncDef := FunctionDef{
			Name:      fnName,
			MemoryMB:  realFunc.MemoryMB,
			InputSize: int64(avgInputSize),
		}

		execTimes := make(map[string]float64)
		initTimes := make(map[string]float64)

		for _, n := range params.EdgeNodes {
			nId := node.NodeID{Area: registration.SelfRegistration.Area, Key: n}

			execTime := 0.3 // Default
			if nodeTimes, ok := retrievedMetrics.AvgEdgeExecutionTime[nId.String()]; ok {
				if t, ok2 := nodeTimes[fnName]; ok2 {
					execTime = t
				}
			}
			execTimes[tupleKey(fnName, n)] = execTime

			pColdEdge := 0.5 // Default
			if prob, ok := retrievedMetrics.EdgeColdStartProbability[fnName]; ok {
				pColdEdge = prob
			}

			avgInitEdge := 0.5 // Default
			if initTimes, ok := retrievedMetrics.AvgEdgeInitTime[nId.String()]; ok {
				if t, ok2 := initTimes[fnName]; ok2 {
					avgInitEdge = t
				}
			}
			initTimes[tupleKey(fnName, n)] = pColdEdge * avgInitEdge
		}
		if len(params.CloudNodes) > 0 {
			if isExternalProviderEnabled {
				cloudNodeID := params.CloudNodes[0] // External

				execTimeCloud := 0.1 // Default
				if t, ok := retrievedMetrics.AvgExtPrvRemoteExecutionTime[fnName]; ok && !math.IsNaN(t) && !math.IsInf(t, 0) {
					execTimeCloud = t
				}
				execTimes[tupleKey(fnName, cloudNodeID)] = execTimeCloud

				pColdCloud := 0.1 // Default
				if p, ok := retrievedMetrics.ExtPrvColdStartProbability[fnName]; ok && !math.IsNaN(p) && !math.IsInf(p, 0) {
					pColdCloud = p
				}

				avgInitCloud := 0.01 // Default
				if t, ok := retrievedMetrics.AvgExtPrvRemoteInitTime[fnName]; ok && !math.IsNaN(t) && !math.IsInf(t, 0) {
					avgInitCloud = t
				}
				initTimes[tupleKey(fnName, cloudNodeID)] = pColdCloud * avgInitCloud
			} else {
				cloudNodeID := params.CloudNodes[0]

				execTimeCloud := 0.1 // Default
				if t, ok := retrievedMetrics.AvgRemoteExecutionTime[fnName]; ok && !math.IsNaN(t) && !math.IsInf(t, 0) {
					execTimeCloud = t
				}
				execTimes[tupleKey(fnName, cloudNodeID)] = execTimeCloud

				pColdCloud := 0.1 // Default
				if p, ok := retrievedMetrics.RemoteColdStartProbability[fnName]; ok && !math.IsNaN(p) && !math.IsInf(p, 0) {
					pColdCloud = p
				}

				avgInitCloud := 0.01 // Default
				if t, ok := retrievedMetrics.AvgRemoteInitTime[fnName]; ok && !math.IsNaN(t) && !math.IsInf(t, 0) {
					avgInitCloud = t
				}
				initTimes[tupleKey(fnName, cloudNodeID)] = pColdCloud * avgInitCloud
			}
		}

		params.Functions = append(params.Functions, newFuncDef)
		for k, v := range execTimes {
			params.ExecTime[k] = v
		}
		for k, v := range initTimes {
			params.InitTime[k] = v
		}

	}

	//Arrival Rates from policy struct
	policy.arrivalRatesMutex.RLock()
	ratesCopy := make(map[string]float64)
	for k, v := range policy.arrivalRates {
		ratesCopy[k] = v
	}
	policy.arrivalRatesMutex.RUnlock()
	params.ArrivalRates = ratesCopy
	end := time.Now().Sub(start)
	log.Printf("Time to prepare payload: %s\n", end)

	return params, nil
}

// LoadQoSDefinitions yaml parsing
func loadQoSDefinitions(filePath string) error {

	// Read yml
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading QoS YAML '%s': %w", filePath, err)
	}

	var configData struct {
		Classes []QoSClass `yaml:"classes"`
	}

	// 3. Esegue il parsing (Unmarshal) del contenuto del file nella struct.
	if err := yaml.Unmarshal(yamlFile, &configData); err != nil {
		return fmt.Errorf("errore nel parsing del file QoS YAML: %w", err)
	}

	for _, classDef := range configData.Classes {
		qosRegistry[classDef.Id] = classDef
	}

	log.Printf("Recovered %d QoS classes with success.", len(qosRegistry))
	return nil
}

func getQoSClassNameByID(id int64) (string, bool) {
	class, found := qosRegistry[id]
	return class.Name, found
}

// GetAllQoSClasses restituisce tutte le classi definite, utile per il payload dell'ottimizzatore.
func getAllQoSClasses() []QoSClass {
	classes := make([]QoSClass, 0, len(qosRegistry))
	for _, classDef := range qosRegistry {
		classes = append(classes, classDef)
	}
	return classes
}

func (policy *IlpOffloadingPolicy) calculateArrivalRates() {
	elapsedSeconds := time.Since(policy.policyStartTime).Seconds()

	if elapsedSeconds == 0 {
		return // Evita divisione per zero
	}

	policy.totalArrivalsMutex.RLock()
	countsSnapshot := make(map[string]int64)
	for key, count := range policy.totalArrivals {
		countsSnapshot[key] = count
	}
	policy.totalArrivalsMutex.RUnlock()

	policy.arrivalRatesMutex.Lock()
	defer policy.arrivalRatesMutex.Unlock()

	for key, totalCount := range countsSnapshot {
		// Tasso medio dall'inizio del test
		avgRate := float64(totalCount) / elapsedSeconds

		oldRate := policy.arrivalRates[key]
		if oldRate == 0 || policy.arrivalAlpha == 1.0 {
			policy.arrivalRates[key] = avgRate
		} else {
			// Con smoothing
			policy.arrivalRates[key] = policy.arrivalAlpha*avgRate + (1.0-policy.arrivalAlpha)*oldRate
		}
	}

	log.Printf("Arrival rates (average since start, elapsed: %.1fs): %v", elapsedSeconds, policy.arrivalRates)
}

func (policy *IlpOffloadingPolicy) PrintDetailedStats() {
	elapsed := time.Since(policy.policyStartTime).Seconds()

	log.Println("\n=== STATISTICHE ARRIVI ===")

	policy.totalArrivalsMutex.RLock()
	var grandTotal int64
	for key, count := range policy.totalArrivals {
		avgRate := float64(count) / elapsed
		grandTotal += count
		log.Printf("  %-40s: %4d richieste → %.3f req/s", key, count, avgRate)
	}
	policy.totalArrivalsMutex.RUnlock()

	log.Printf("  TOTALE: %d richieste → %.3f req/s complessivi", grandTotal, float64(grandTotal)/elapsed)
	log.Printf("  Durata test: %.1f secondi\n", elapsed)
}

func tupleKey(s1, s2 string) string {
	keyBytes, _ := json.Marshal([]string{s1, s2})
	return string(keyBytes)
}

func (policy *IlpOffloadingPolicy) needProfiling(minExecutionNumber, intervalSeconds int) map[string]map[int]bool {
	profilingThreshold := 0
	profilingMap := make(map[string]map[int]bool)

	functionsToMonitor, err := function.GetAll()

	if err != nil {
		log.Printf("Error getting functions: %v", err)
		return nil
	}

	for _, funcName := range functionsToMonitor {

		increaseMap, err := metrics.QueryIncreaseForFunction(funcName, intervalSeconds)
		if err != nil {
			log.Printf("Error querying increase for function '%s': %v", funcName, err)
			continue
		}

		// Variabili per i conteggi
		var localExecutions = 0
		var edgeExecutions = 0
		var lambdaExecutions = 0
		var cloudExecutions = 0

		cloudNode := registration.GetRemoteOffloadingTarget()
		var cloudID string
		if cloudNode != nil {
			cloudID = cloudNode.NodeID.String()
		}

		for nodeId, count := range increaseMap {
			if strings.Contains(nodeId, utils.ExternalProvider) {
				lambdaExecutions += count
			} else if strings.Contains(nodeId, node.LocalNode.String()) {
				localExecutions += count
			} else if cloudID != "" && strings.Contains(nodeId, cloudID) {
				cloudExecutions += count
			} else {
				edgeExecutions += count
			}
		}

		totalExecutionsForFunc := localExecutions + edgeExecutions + lambdaExecutions + cloudExecutions

		log.Printf("Executions for func '%s':", funcName)
		log.Printf("  Total: %d (Threshold: %d)", totalExecutionsForFunc, profilingThreshold)
		log.Printf("  -> Local: %d, Edge: %d, Lambda: %d, Cloud: %d", localExecutions, edgeExecutions, lambdaExecutions, cloudExecutions)
		if totalExecutionsForFunc >= profilingThreshold {
			log.Printf("  -> Profiling threshold REACHED for '%s'. Checking areas...", funcName)

			profilingMap[funcName] = make(map[int]bool)

			// Controlla se è necessario il profiling per ciascuna area
			if lambdaExecutions <= minExecutionNumber {
				log.Printf("     -> Profiling needed for LAMBDA")
				profilingMap[funcName][LAMBDA] = true
			}
			if cloudExecutions <= minExecutionNumber {
				log.Printf(" -> Profiling needed for CLOUD")
				profilingMap[funcName][CLOUD] = true
			}
			if edgeExecutions <= minExecutionNumber {
				log.Printf("     -> Profiling needed for EDGE")
				profilingMap[funcName][EDGE] = true
			}
			if localExecutions <= minExecutionNumber {
				log.Printf("     -> Profiling needed for LOCAL")
				profilingMap[funcName][LOCAL] = true
			}
		}
		log.Println("--------------------")
	}
	return profilingMap
}

func PrintOptimizerPayload(params optimizerPayload) {
	var sb strings.Builder

	sb.WriteString("\n\n==========================================================\n")
	sb.WriteString("---   Snapshot del Payload per l'Ottimizzatore   ---\n")
	sb.WriteString("==========================================================\n")

	// --- 1. Definizioni Statiche ---
	sb.WriteString("\n=== 1. Definizioni Statiche (da Config/YAML) ===\n")
	fmt.Fprintf(&sb, "Funzioni definite (%d):\n", len(params.Functions))
	if len(params.Functions) == 0 {
		sb.WriteString("  (Nessuna funzione trovata)\n")
	}
	for _, f := range params.Functions {
		fmt.Fprintf(&sb, "  - Nome: %-25s | Memoria Richiesta: %d MB\n", f.Name, f.MemoryMB)
	}

	fmt.Fprintf(&sb, "\nClassi QoS definite (%d):\n", len(params.Classes))
	if len(params.Classes) == 0 {
		sb.WriteString("  (Nessuna classe QoS trovata)\n")
	}
	for _, c := range params.Classes {
		fmt.Fprintf(&sb, "  - ID: %-2d | Nome: %-15s | MaxResp: %.2fs | Utility: %.2f | DeadlinePenalty: %.2f | DropPenalty: %.2f\n",
			c.Id, c.Name, c.MaxRespTime, c.Utility, c.DeadlinePenalty, c.DropPenalty)
	}

	// --- 2. Parametri della Policy ---
	sb.WriteString("\n=== 2. Parametri della Policy (da Config/INI) ===\n")
	fmt.Fprintf(&sb, "Budget:                 %.4f $/ora\n", params.Budget)
	fmt.Fprintf(&sb, "Costo Cloud (GB/sec):   %.8f\n", params.CloudComputeCostGbSec)
	fmt.Fprintf(&sb, "Costo Cloud (richiesta): %.8f\n", params.CloudRequestCost)
	fmt.Fprintf(&sb, "Costi per Cloud generico :\n", params.GenericCloudCost)

	// --- 3. Dati Dinamici "Live" ---
	sb.WriteString("\n=== 3. Dati Dinamici \"Live\" (da Stato del Sistema e Prometheus) ===\n")
	fmt.Fprintf(&sb, "Nodo Gestore:          %s\n", params.HandlingNode)
	fmt.Fprintf(&sb, "Nodi Edge Attivi:      %v\n", params.EdgeNodes)
	fmt.Fprintf(&sb, "Nodi Cloud:            %v\n", params.CloudNodes)
	fmt.Fprintf(&sb, "Memoria Aggregata Edge: %.2f MB\n", params.AggregatedEdgeMemory)

	fmt.Fprintf(&sb, "\nMemoria Disponibile per Nodo (%d voci):\n", len(params.NodeMemory))
	for node, mem := range params.NodeMemory {
		fmt.Fprintf(&sb, "  - %-30s: %.2f MB\n", node, mem)
	}

	fmt.Fprintf(&sb, "\nLatenza RTT tra Nodi (%d voci):\n", len(params.NodeLatency))
	for nodes, lat := range params.NodeLatency {
		fmt.Fprintf(&sb, "  - %-30s: %.4f s\n", nodes, lat)
	}

	fmt.Fprintf(&sb, "\nBanda verso l'Edge: %.2f Mbps\n", params.BandwidthEdge)

	fmt.Fprintf(&sb, "\nTempi di Esecuzione Medi (ExecTime) (%d voci):\n", len(params.ExecTime))
	for key, val := range params.ExecTime {
		fmt.Fprintf(&sb, "  - %-40s: %.6f s\n", key, val)
	}

	fmt.Fprintf(&sb, "\nTempi di Inizializzazione Attesi (InitTime) (%d voci):\n", len(params.InitTime))
	for key, val := range params.InitTime {
		fmt.Fprintf(&sb, "  - %-40s: %.6f s\n", key, val)
	}

	fmt.Fprintf(&sb, "\nTassi di Arrivo (ArrivalRates) (%d voci):\n", len(params.ArrivalRates))
	if len(params.ArrivalRates) == 0 {
		sb.WriteString("  (Nessun tasso di arrivo misurato)\n")
	}
	for key, val := range params.ArrivalRates {
		fmt.Fprintf(&sb, "  - %-30s: %.6f req/s\n", key, val)
	}

	sb.WriteString("\n==========================================================\n\n")

	// Stampa l'intera stringa costruita nel log
	log.Println(sb.String())
}
