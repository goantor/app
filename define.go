package app

import "context"

type ServiceKind int

type IServices []IService
type IService interface {
	TakeName() string
	Boot() error
	Shutdown(ctx context.Context) error
}
