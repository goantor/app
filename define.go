package app

type ServiceKind int

type IServices []IService
type IService interface {
	TakeName() string
	Boot() error
}
