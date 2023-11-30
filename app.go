package app

import (
	"context"
	"github.com/goantor/pr"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type IApp interface {
	RegisterServices(services ...IService) IApp
	Boot() error
}

func NewApp(services ...IService) IApp {
	if services == nil || len(services) == 0 {
		services = make(IServices, 0)
	}

	return &app{
		services: services,
	}
}

type app struct {
	services IServices
}

func (a *app) RegisterServices(services ...IService) IApp {
	a.services = append(a.services, services...)
	return a
}

func (a *app) boot() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	for _, serv := range a.services {
		go func(service IService) {
			pr.Green("Service %s start....", service.TakeName())
			if err := service.Boot(); err != nil {
				sig <- syscall.SIGINT
				pr.Red("cancel service failed: %s\n", err.Error())
			}
		}(serv)
	}
	<-sig
}

func (a *app) shutdown() {
	pr.Green("Received shutdown signal. Shutting down server...\n")

	// 设置优雅关闭的最大时间
	gracefulShutdownTimeout := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()

	for _, serv := range a.services {
		if err := serv.Shutdown(ctx); err != nil {
			pr.Red("Failed to gracefully shutdown service: %v\n", err)
		}
	}

	pr.Green("Server is shut down. Exiting...\n")
}

func (a *app) Boot() error {
	a.boot()
	a.shutdown()
	return nil
}
