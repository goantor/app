package app

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

type IWebServiceOption interface {
	TakeName() string
	TakeHost() string
	TakePort() int
	TakeReadTimeout() time.Duration
	TakeWriteTimeout() time.Duration
}

type WebRouteFunc func(route *gin.Engine)

func NewWebService(opt IWebServiceOption, route WebRouteFunc) *WebService {
	return &WebService{
		handle: gin.New(),
		opt:    opt,
		routes: route,
	}
}

type WebService struct {
	handle *gin.Engine
	opt    IWebServiceOption
	routes WebRouteFunc
}

func (s WebService) Boot() error {
	server := &http.Server{
		Handler:      s.handle,
		Addr:         fmt.Sprintf("%s:%d", s.opt.TakeHost(), s.opt.TakePort()),
		ReadTimeout:  s.opt.TakeReadTimeout(),
		WriteTimeout: s.opt.TakeWriteTimeout(),
	}

	s.routes(s.handle)
	return server.ListenAndServe()
}
