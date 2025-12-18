package lambda

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/commonutils"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda/utils"
)

type RTTBatchOpts struct {
	Attempts  int     // Total number of invocations per measurement
	Warmup    int     // How many to discard at beginning
	TrimRatio float64 // % to cut in up/down (outliers)
	Timeout   time.Duration
}

type RttMonitor struct {
	latestRtt    time.Duration
	mutex        sync.RWMutex
	lambdaFnName string
	// Cache the client to avoid reloading config every second
	client *lambda.Client
}

var (
	defaultRttMonitor *RttMonitor
	initOnce          sync.Once
)

// InitRttMonitor initializes the RTT monitor singleton safely.
func InitRttMonitor(updateInterval time.Duration, fnArn string) error {
	var err error
	initOnce.Do(func() {
		// Create a specific client without retries for accurate measurement
		cli, cliErr := newNoRetryLambdaClient(context.Background())
		if cliErr != nil {
			err = fmt.Errorf("failed to create lambda client: %w", cliErr)
			return
		}

		log.Printf("Starting RTT Monitor. Update every %v.", updateInterval)
		monitor := &RttMonitor{
			lambdaFnName: fnArn,
			latestRtt:    40 * time.Millisecond, // Default conservative value
			client:       cli,
		}
		defaultRttMonitor = monitor

		// Ensure the target function exists before starting the loop
		if err := EnsurePingFunction(context.Background(), fnArn); err != nil {
			log.Printf("Warning: Failed to ensure ping function exists: %v. Monitor will retry later.", err)
		}

		// Start background polling
		go monitor.monitorLoop(updateInterval)
	})
	return err
}

func (p FunctionProvider) GetRtt() time.Duration {
	if defaultRttMonitor == nil {
		// Fallback safe value if monitor is not initialized
		return 40 * time.Millisecond
	}
	return defaultRttMonitor.get()
}

func (m *RttMonitor) get() time.Duration {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.latestRtt
}

func (m *RttMonitor) monitorLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Immediate first measurement
	m.updateRtt()

	for range ticker.C {
		m.updateRtt()
	}
}

func (m *RttMonitor) updateRtt() {
	newRtt, err := m.measure()
	if err != nil {
		log.Printf("Error measuring RTT: %v", err)
		return
	}

	m.mutex.Lock()
	m.latestRtt = newRtt
	m.mutex.Unlock()

}

func (m *RttMonitor) measure() (time.Duration, error) {
	opts := RTTBatchOpts{Attempts: 5, Warmup: 1, TrimRatio: 0.2, Timeout: 2 * time.Second}

	invokeOnce := func(ctx context.Context) (time.Duration, bool, error) {
		cctx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()

		in := &lambda.InvokeInput{
			FunctionName:   aws.String(m.lambdaFnName),
			InvocationType: types.InvocationTypeRequestResponse,
			LogType:        types.LogTypeTail, // Essential to get execution duration
			Payload:        []byte(`{"ping":true}`),
		}

		start := time.Now()
		out, err := m.client.Invoke(cctx, in)
		if err != nil {
			return 0, false, err
		}
		totalRoundTrip := time.Since(start)

		// Check for Cold Start
		if _, isCold := utils.ExtractInitDurationFromLog(out.LogResult); isCold {
			return 0, true, nil
		}

		// Extract AWS Execution Time
		var handlerDurationMs float64
		if d, ok := utils.ExtractDurationFromLog(out.LogResult); ok {
			handlerDurationMs = d
		}

		// Calculate Transport Time: Total - Execution
		transport := totalRoundTrip - time.Duration(handlerDurationMs*float64(time.Millisecond))
		if transport < 0 {
			transport = 0
		}
		return transport, false, nil
	}

	// Collect samples
	vals := make([]time.Duration, 0, opts.Attempts)
	for i := 0; i < opts.Attempts; i++ {
		d, cold, err := invokeOnce(context.Background())
		// We skip errors and cold starts to get steady-state network latency
		if err != nil || cold {
			continue
		}
		// Skip warmup attempts
		if i < opts.Warmup {
			continue
		}
		vals = append(vals, d)
	}

	if len(vals) == 0 {
		return 0, fmt.Errorf("no warm samples collected")
	}

	// Filter outliers (Median calculation logic)
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	trim := int(float64(len(vals)) * opts.TrimRatio)
	if 2*trim < len(vals) {
		vals = vals[trim : len(vals)-trim]
	}

	// Average the remaining middle values
	var sum time.Duration
	for _, v := range vals {
		sum += v
	}
	return sum / time.Duration(len(vals)), nil
}

func EnsurePingFunction(ctx context.Context, fnName string) error {
	provider, err := GetProvider()
	if err != nil {
		return fmt.Errorf("error getting provider: %w", err)
	}

	// 1. Check if exists
	list, err := provider.ListFunctions(ctx)
	if err != nil {
		return fmt.Errorf("error listing functions: %w", err)
	}

	for _, name := range list {
		if name == fnName {
			return nil
		}
	}

	// 2. Create if missing
	log.Printf("Ping function %q not found, creating...", fnName)

	code := `def lambda_handler(event, context): return {"pong": True}`
	zipBytes, err := CreateZipFromCode(code, "lambda_function.py")
	if err != nil {
		return fmt.Errorf("failed to create zip: %w", err)
	}

	input := &lambda.CreateFunctionInput{
		FunctionName: aws.String(fnName),
		Runtime:      types.RuntimePython310,
		Role:         aws.String(provider.role),
		Handler:      aws.String("lambda_function.lambda_handler"),
		Code: &types.FunctionCode{
			ZipFile: zipBytes,
		},
		MemorySize:    aws.Int32(128),
		Timeout:       aws.Int32(3),
		Publish:       true,
		Architectures: []types.Architecture{types.ArchitectureX8664}, // Explicit architecture is safer
	}

	_, err = provider.client.CreateFunction(ctx, input)
	if err != nil {
		return fmt.Errorf("error creating ping function: %w", err)
	}

	log.Printf("Ping function %q created successfully.", fnName)
	return nil
}

func newNoRetryLambdaClient(ctx context.Context) (*lambda.Client, error) {
	base, err := commonutils.LoadAWSConfig()
	if err != nil {
		return nil, err
	}
	// Load config ensuring we disable retries
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(base.Region),
		config.WithCredentialsProvider(base.Credentials),
		config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	if err != nil {
		return nil, err
	}
	return lambda.NewFromConfig(cfg), nil
}
