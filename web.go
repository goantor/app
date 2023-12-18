package app

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

//type IRegistry

type WebRouteRegistry map[string]WebRouteFunc

type IWebOption interface {
	TakeName() string
	TakeHost() string
	TakePort() int
	TakeReadTimeout() time.Duration
	TakeWriteTimeout() time.Duration
}

type WebRouteFunc func(route *gin.Engine)

func NewWeb(opt IWebOption, route WebRouteFunc) IService {
	return &Web{
		handle: gin.New(),
		opt:    opt,
		routes: route,
	}
}

type Web struct {
	handle *gin.Engine
	opt    IWebOption
	routes WebRouteFunc
	server *http.Server
}

func (s *Web) TakeName() string {
	return s.opt.TakeName()
}

func (s *Web) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Web) Boot() error {
	s.server = &http.Server{
		Handler:      s.handle,
		Addr:         fmt.Sprintf("%s:%d", s.opt.TakeHost(), s.opt.TakePort()),
		ReadTimeout:  s.opt.TakeReadTimeout(),
		WriteTimeout: s.opt.TakeWriteTimeout(),
	}

	s.routes(s.handle)
	return s.server.ListenAndServe()
}
