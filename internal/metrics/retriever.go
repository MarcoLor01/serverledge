package metrics

import (
	"context"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/externalprovider"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda/utils"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"log"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/serverledge-faas/serverledge/internal/config"

	promapi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

var retrievedMetrics RetrievedMetrics
var metricsLock sync.RWMutex

type metricSample struct {
	Value  float64
	Labels map[string]string
}
type metricProcessor[T any] func(samples []metricSample) (T, error)

func executeQuery(query string, api v1.API, ctx context.Context) (model.Vector, error) {
	result, warnings, err := api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed query: %v", err)
	}

	if len(warnings) > 0 {
		log.Printf("received warnings in the execution: %v", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("could not convert the result of the query: %v", result)
	}

	return vector, nil
}

func extractSampleWithLabels(sample *model.Sample, requiredLabels []string) (*metricSample, error) {
	labels := make(map[string]string)

	for _, labelName := range requiredLabels {
		labelValue, found := sample.Metric[model.LabelName(labelName)]
		if !found {
			return nil, fmt.Errorf("could not find the %s label in the result: %v", labelName, sample)
		}
		labels[labelName] = string(labelValue)
	}

	return &metricSample{
		Value:  float64(sample.Value),
		Labels: labels,
	}, nil
}

func retrieveMetrics[T any](query string, api v1.API, ctx context.Context, requiredLabels []string, processor metricProcessor[T]) (T, error) {
	var zero T

	vector, err := executeQuery(query, api, ctx)
	if err != nil {
		return zero, err
	}

	var samples []metricSample
	for _, sample := range vector {
		extracted, err := extractSampleWithLabels(sample, requiredLabels)
		if err != nil {
			log.Printf("skipping sample: %v", err)
			continue
		}
		samples = append(samples, *extracted)
	}

	return processor(samples)
}

func retrieveSingleValue(query string, api v1.API, ctx context.Context) (float64, error) {
	return retrieveMetrics(query, api, ctx, []string{}, func(samples []metricSample) (float64, error) {
		if len(samples) != 1 {
			// This will cause the function to return zero value, but we should handle this better
			return 0.0, fmt.Errorf("Expected 1 result; found %d\n", len(samples))
		}
		return samples[0].Value, nil
	})
}

func retrieveByFunction(query string, api v1.API, ctx context.Context) (map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"function"}, func(samples []metricSample) (map[string]float64, error) {
		result := make(map[string]float64)
		for _, sample := range samples {
			result[sample.Labels["function"]] = sample.Value
		}
		return result, nil
	})
}

func retrieveByFunctionAndNode(query string, api v1.API, ctx context.Context) (map[string]map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"function", "node"}, func(samples []metricSample) (map[string]map[string]float64, error) {
		result := make(map[string]map[string]float64)
		for _, sample := range samples {
			nodeName := sample.Labels["node"]
			functionName := sample.Labels["function"]

			if _, exists := result[nodeName]; !exists {
				result[nodeName] = make(map[string]float64)
			}
			result[nodeName][functionName] = sample.Value
		}
		return result, nil
	})
}

func retrieveByTaskAndNextTask(query string, api v1.API, ctx context.Context) (map[string]map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"task", "next_task"}, func(samples []metricSample) (map[string]map[string]float64, error) {
		values := make(map[string]map[string]float64)

		// Build the raw values map
		for _, sample := range samples {
			taskId := sample.Labels["task"]
			nextTaskId := sample.Labels["next_task"]

			if _, exists := values[taskId]; !exists {
				values[taskId] = make(map[string]float64)
			}
			values[taskId][nextTaskId] = sample.Value
		}

		// Normalize to probabilities
		for taskId, innerMap := range values {
			sum := 0.0
			for _, value := range innerMap {
				sum += value
			}

			if sum > 0 {
				for nextTaskId, value := range innerMap {
					values[taskId][nextTaskId] = value / sum
				}
			}
		}

		return values, nil
	})
}

func retrieveCompletionsByFunctionAndNode(query string, api v1.API, ctx context.Context) (map[string]map[string]int, error) {
	return retrieveMetrics(query, api, ctx, []string{"function", "node"}, func(samples []metricSample) (map[string]map[string]int, error) {
		result := make(map[string]map[string]int)
		for _, sample := range samples {
			funcName := sample.Labels["function"]
			node := sample.Labels["node"]
			if _, exists := result[funcName]; !exists {
				result[funcName] = make(map[string]int)
			}
			result[funcName][node] = int(sample.Value)
		}
		return result, nil
	})
}

func QueryIncreaseForFunction(funcName string, intervalSeconds int) (map[string]int, error) {
	prometheusHost := config.GetString(config.METRICS_PROMETHEUS_HOST, "127.0.0.1")
	prometheusPort := config.GetInt(config.METRICS_PROMETHEUS_PORT, 9090)
	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:%d", prometheusHost, prometheusPort),
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Prometheus client: %w", err)
	}
	api := v1.NewAPI(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Query for completions
	query := fmt.Sprintf(`increase(completed_count{function="%s"}[%ds])`, funcName, intervalSeconds)

	vector, err := executeQuery(query, api, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute increase query: %w", err)
	}

	increaseMap := make(map[string]int)
	for _, sample := range vector {
		nodeId, found := sample.Metric[model.LabelName("node")]
		if !found {
			continue
		}
		increaseMap[string(nodeId)] = int(sample.Value)
	}

	return increaseMap, nil
}

func MetricsRetriever() {
	prometheusHost := config.GetString(config.METRICS_PROMETHEUS_HOST, "127.0.0.1")
	prometheusPort := config.GetInt(config.METRICS_PROMETHEUS_PORT, 9090)
	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:%d", prometheusHost, prometheusPort),
	})
	if err != nil {
		log.Printf("Error in Prometheus client creation: %v\n", err)
		return
	}

	// API of Prometheus
	api := v1.NewAPI(client)
	ctx := context.Background()

	var lambdaProvider externalprovider.FunctionProvider
	externalProviderEnabled := config.GetBool(config.EXTERNAL_PROVIDER_ENABLED, false)
	if externalProviderEnabled {
		p, err := externalprovider.NewFunctionOffloader(externalprovider.LambdaOffloader)
		if err != nil {
			log.Printf("Error initializing External FunctionProvider: %v", err)
		} else {
			lambdaProvider = p
		}
	}

	ticker := time.NewTicker(time.Duration(config.GetInt(config.METRICS_RETRIEVER_INTERVAL, 60)) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			var newMetrics RetrievedMetrics

			newMetrics.AvgEdgeExecutionTime = make(map[string]map[string]float64)
			newMetrics.AvgEdgeInitTime = make(map[string]map[string]float64)
			newMetrics.BranchFrequency = make(map[string]map[string]float64)
			newMetrics.CompletionsByFunctionAndNode = make(map[string]map[string]int)
			newMetrics.AvgOutputSize = make(map[string]float64)
			newMetrics.AvgInputSize = make(map[string]float64)
			newMetrics.EdgeColdStartProbability = make(map[string]float64)
			newMetrics.RemoteColdStartProbability = make(map[string]float64)
			newMetrics.AvgRemoteExecutionTime = make(map[string]float64)
			newMetrics.AvgRemoteInitTime = make(map[string]float64)
			newMetrics.ExtPrvColdStartProbability = make(map[string]float64)
			newMetrics.AvgExtPrvRemoteExecutionTime = make(map[string]float64)
			newMetrics.AvgExtPrvRemoteInitTime = make(map[string]float64)
			newMetrics.ArrivalRates = make(map[string]float64)

			query := fmt.Sprintf("%s_sum{}/%s_count{}", OUTPUT_SIZE, OUTPUT_SIZE)
			avgOutputSize, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v", err)
			}
			newMetrics.AvgOutputSize = avgOutputSize

			query = fmt.Sprintf("%s_sum{}/%s_count{}", INPUT_SIZE, INPUT_SIZE)
			avgInputSize, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v", err)
			}
			newMetrics.AvgInputSize = avgInputSize

			query = fmt.Sprintf("%s{}", BRANCH_COUNT)
			frequencyPerTaskAndNextOne, err := retrieveByTaskAndNextTask(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByTaskAndNextTask: %v\n", err)
			}
			newMetrics.BranchFrequency = frequencyPerTaskAndNextOne

			//Completions for nodes
			query = fmt.Sprintf("%s{}", COMPLETIONS)
			localArea := registration.SelfRegistration.Area
			CompletionsByFunctionAndNode, err := retrieveCompletionsByFunctionAndNode(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveCompletionsByFunctionAndNode: %v", err)
			} else {
				newMetrics.CompletionsByFunctionAndNode = CompletionsByFunctionAndNode
			}

			// Execution time on Edge peers
			localArea = registration.SelfRegistration.Area
			query = fmt.Sprintf("%s_sum{node=~\"\\\\(%s\\\\).*\"}/%s_count{node=~\"\\\\(%s\\\\).*\"}",
				EXECUTION_TIME, localArea, EXECUTION_TIME, localArea)
			avgFunDurationAllNodes, err := retrieveByFunctionAndNode(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v", err)
			}
			newMetrics.AvgEdgeExecutionTime = avgFunDurationAllNodes

			query = fmt.Sprintf("%s_sum{node=~\"\\\\(%s\\\\).*\"}/%s_count{node=~\"\\\\(%s\\\\).*\"}",
				INITIALIZATION_TIME, localArea, INITIALIZATION_TIME, localArea)
			avgInitTimeAllNodes, err := retrieveByFunctionAndNode(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v", err)
			}
			newMetrics.AvgEdgeInitTime = avgInitTimeAllNodes

			//Probability Cold Start Edge
			query = fmt.Sprintf("%s{area=\"%s\"}/%s{area=\"%s\"}",
				COLD_STARTS, localArea, COMPLETIONS, localArea)
			edgeColdStartProb, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v", err)
			}
			newMetrics.EdgeColdStartProbability = edgeColdStartProb

			// CLOUD
			cloudArea := config.GetString(config.REGISTRY_REMOTE_AREA, "")
			if cloudArea != "" {
				query = fmt.Sprintf("%s{area=\"%s\"}/%s{area=\"%s\"}", COLD_STARTS, cloudArea, COMPLETIONS, cloudArea)
				coldStartProbPerFunction, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Error in retrieveByFunction: %v", err)
				}
				newMetrics.RemoteColdStartProbability = coldStartProbPerFunction

				query = fmt.Sprintf("%s_sum{node=~\"\\\\(%s\\\\).*\"}/%s_count{node=~\"\\\\(%s\\\\).*\"}",
					EXECUTION_TIME, cloudArea, EXECUTION_TIME, cloudArea)
				avgFunDuration, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Error in retrieveByFunction: %v", err)
				}
				newMetrics.AvgRemoteExecutionTime = avgFunDuration

				query = fmt.Sprintf("%s_sum{node=~\"\\\\(%s\\\\).*\"}/%s_count{node=~\"\\\\(%s\\\\).*\"}",
					INITIALIZATION_TIME, cloudArea, INITIALIZATION_TIME, cloudArea)
				avgInitTime, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Error in retrieveByFunction: %v", err)
				}
				newMetrics.AvgRemoteInitTime = avgInitTime
			} else {
				newMetrics.AvgRemoteExecutionTime = make(map[string]float64)
				newMetrics.AvgRemoteInitTime = make(map[string]float64)
			}

			//EXTERNAL PROVIDER
			if lambdaProvider != nil {
				region, err := lambdaProvider.GetRegion()
				if err != nil {
					log.Printf("Errore nel recupero della regione per External FunctionProvider: %v\n", err)
				}

				extProviderName := utils.ExternalProvider
				extNodeLabel := fmt.Sprintf("%s:%s", extProviderName, region)

				query = fmt.Sprintf(
					`sum by(function) (%s{area="%s"}) / sum by(function) (%s{area="%s"})`,
					COLD_STARTS, extProviderName, COMPLETIONS, extProviderName,
				)
				coldStartProbPerFunction, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Errore nel recupero di ExtPrvColdStartProbability: %v", err)
				}
				newMetrics.ExtPrvColdStartProbability = coldStartProbPerFunction

				query = fmt.Sprintf(
					`%s_sum{node="%s"} / %s_count{node="%s"}`,
					EXECUTION_TIME, extNodeLabel, EXECUTION_TIME, extNodeLabel,
				)
				avgFunDuration, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Errore nel recupero di AvgExtPrvRemoteExecutionTime: %v", err)
				}
				newMetrics.AvgExtPrvRemoteExecutionTime = avgFunDuration

				query = fmt.Sprintf(
					`%s_sum{node="%s"} / %s_count{node="%s"}`,
					INITIALIZATION_TIME, extNodeLabel, INITIALIZATION_TIME, extNodeLabel,
				)
				avgInitTime, err := retrieveByFunction(query, api, ctx)
				if err != nil {
					log.Printf("Errore nel recupero di AvgExtPrvRemoteInitTime: %v", err)
				}
				newMetrics.AvgExtPrvRemoteInitTime = avgInitTime
			}

			fmt.Println("All queries completed")
			fmt.Println(newMetrics)

			metricsLock.Lock()
			retrievedMetrics = newMetrics // Sostituzione atomica
			metricsLock.Unlock()
		}
	}

}

func GetMetrics() RetrievedMetrics {
	metricsLock.RLock()
	defer metricsLock.RUnlock()

	copied := RetrievedMetrics{
		ExtPrvColdStartProbability:   make(map[string]float64, len(retrievedMetrics.ExtPrvColdStartProbability)),
		AvgExtPrvRemoteExecutionTime: make(map[string]float64, len(retrievedMetrics.AvgExtPrvRemoteExecutionTime)),
		AvgExtPrvRemoteInitTime:      make(map[string]float64, len(retrievedMetrics.AvgExtPrvRemoteInitTime)),
		RemoteColdStartProbability:   make(map[string]float64, len(retrievedMetrics.RemoteColdStartProbability)),
		AvgRemoteExecutionTime:       make(map[string]float64, len(retrievedMetrics.AvgRemoteExecutionTime)),
		AvgEdgeExecutionTime:         make(map[string]map[string]float64, len(retrievedMetrics.AvgEdgeExecutionTime)),
		AvgRemoteInitTime:            make(map[string]float64, len(retrievedMetrics.AvgRemoteInitTime)),
		AvgEdgeInitTime:              make(map[string]map[string]float64, len(retrievedMetrics.AvgEdgeInitTime)),
		EdgeColdStartProbability:     make(map[string]float64, len(retrievedMetrics.EdgeColdStartProbability)),
		AvgInputSize:                 make(map[string]float64, len(retrievedMetrics.AvgInputSize)),
		AvgOutputSize:                make(map[string]float64, len(retrievedMetrics.AvgOutputSize)),
		BranchFrequency:              make(map[string]map[string]float64, len(retrievedMetrics.BranchFrequency)),
		ArrivalRates:                 make(map[string]float64, len(retrievedMetrics.ArrivalRates)),
		CompletionsByFunctionAndNode: make(map[string]map[string]int, len(retrievedMetrics.CompletionsByFunctionAndNode)),
	}

	// 3. Copia i valori delle mappe a singolo livello
	for k, v := range retrievedMetrics.ExtPrvColdStartProbability {
		copied.ExtPrvColdStartProbability[k] = v
	}
	for k, v := range retrievedMetrics.AvgExtPrvRemoteExecutionTime {
		copied.AvgExtPrvRemoteExecutionTime[k] = v
	}
	for k, v := range retrievedMetrics.AvgExtPrvRemoteInitTime {
		copied.AvgExtPrvRemoteInitTime[k] = v
	}
	for k, v := range retrievedMetrics.RemoteColdStartProbability {
		copied.RemoteColdStartProbability[k] = v
	}
	for k, v := range retrievedMetrics.AvgRemoteExecutionTime {
		copied.AvgRemoteExecutionTime[k] = v
	}
	for k, v := range retrievedMetrics.AvgRemoteInitTime {
		copied.AvgRemoteInitTime[k] = v
	}
	for k, v := range retrievedMetrics.EdgeColdStartProbability {
		copied.EdgeColdStartProbability[k] = v
	}
	for k, v := range retrievedMetrics.AvgInputSize {
		copied.AvgInputSize[k] = v
	}
	for k, v := range retrievedMetrics.AvgOutputSize {
		copied.AvgOutputSize[k] = v
	}
	for k, v := range retrievedMetrics.ArrivalRates {
		copied.ArrivalRates[k] = v
	}

	// 4. Copia i valori delle mappe annidate (float64)
	for k1, v := range retrievedMetrics.AvgEdgeExecutionTime {
		copied.AvgEdgeExecutionTime[k1] = make(map[string]float64, len(v))
		for k2, v2 := range v {
			copied.AvgEdgeExecutionTime[k1][k2] = v2
		}
	}

	for k1, v := range retrievedMetrics.AvgEdgeInitTime {
		copied.AvgEdgeInitTime[k1] = make(map[string]float64, len(v))
		for k2, v2 := range v {
			copied.AvgEdgeInitTime[k1][k2] = v2
		}
	}

	for k1, v := range retrievedMetrics.BranchFrequency {
		copied.BranchFrequency[k1] = make(map[string]float64, len(v))
		for k2, v2 := range v {
			copied.BranchFrequency[k1][k2] = v2
		}
	}

	// 5. Copia i valori della mappa annidata (int)
	for k1, v := range retrievedMetrics.CompletionsByFunctionAndNode {
		copied.CompletionsByFunctionAndNode[k1] = make(map[string]int, len(v))
		for k2, v2 := range v {
			copied.CompletionsByFunctionAndNode[k1][k2] = v2
		}
	}

	// 6. Restituisci la copia profonda e sicura
	return copied
}
