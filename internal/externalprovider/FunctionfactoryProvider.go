package externalprovider

import (
	"context"
	"fmt"
	"github.com/serverledge-faas/serverledge/internal/externalprovider/lambda"
	"github.com/serverledge-faas/serverledge/internal/function"
	"time"
)

type FunctionProvider interface {
	CreateFunction(ctx context.Context, function *function.Function, arch string) (string, error)
	ListFunctions(ctx context.Context) ([]string, error)
	InvokeProviderFunction(request *function.Request, payload []byte) (function.ExecutionReport, error)
	DeleteProviderFunction(ctx context.Context, function *function.Function) error
	GetRegion() (string, error)
	GetRtt() time.Duration
}

const LambdaOffloader = "aws-lambdafunction"

func NewFunctionOffloader(name string) (FunctionProvider, error) {
	switch name {
	case LambdaOffloader:
		p, err := lambda.GetProvider()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize AWS provider: %w", err)
		}
		return p, nil

	//Here we can add other provider

	default:
		return nil, fmt.Errorf("unknown offload provider %q", name)
	}
}
