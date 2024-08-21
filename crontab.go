package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/goantor/x"
	"github.com/robfig/cron/v3"
	"runtime/debug"
)

// cronTab :=
//
//	// 定义定时器调用的任务函数
//	task := func() {
//		fmt.Println("hello world", time.Now())
//	}
//	// 定时任务,cron表达式,每五秒一次
//	spec := "* * * * * ?"
//	// 添加定时任务
//	cronTab.AddFunc(spec, task)
//	// 启动定时器
//	cronTab.Start()
//	// 阻塞主线程停止
//	select {}

type ICrontabRouteRegistry interface {
	Register(name string, handler TaskHandler)
	Take(name string) (TaskHandler, bool)
}

func NewCrontabRouteRegistry() ICrontabRouteRegistry {
	return CrontabRouteRegistry{}
}

type CrontabRouteRegistry map[string]TaskHandler

func (c CrontabRouteRegistry) Take(name string) (TaskHandler, bool) {
	handler, ok := c[name]
	return handler, ok
}

func (c CrontabRouteRegistry) Register(name string, handler TaskHandler) {
	c[name] = handler
}

type CrontabKind int

type TaskHandler func(ctx x.Context) error

type CrontabRouteFunc func(routes ICrontabRoutes)

type ICrontabRoutes interface {
	RegisterCron(route *CrontabRoute)
	HasNext() bool
	Next() *CrontabRoute
}

func NewCrontabRoutes() ICrontabRoutes {
	return &CrontabRoutes{
		routes: make([]*CrontabRoute, 0),
		index:  0,
	}
}

type CrontabRoutes struct {
	routes []*CrontabRoute
	index  int
}

func (o *CrontabRoutes) RegisterCron(route *CrontabRoute) {
	o.routes = append(o.routes, route)
}

func (o *CrontabRoutes) HasNext() bool {
	return o.index < len(o.routes)
}

func (o *CrontabRoutes) Next() *CrontabRoute {
	if o.HasNext() {
		route := o.routes[o.index]
		o.index++
		return route
	}

	return nil
}

type CrontabRoute struct {
	Name             string
	Handler          TaskHandler
	Spec             string
	StillRunningType int
}

func (cr *CrontabRoute) makeCronHandler(name string, route *CrontabRoute, log x.ILogger) (handle func()) {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				err := errors.New(fmt.Sprintf("crontab catch error: %v", r))
				log.Error("crontab run failed", err, x.H{
					"error":      r,
					"debugStack": string(debug.Stack()),
					"name":       name,
				})
			}
		}()

		ctx := x.NewContext(log)
		ctx.GiveService("crontab")
		ctx.GiveModule(name)
		if err := route.Handler(ctx); err != nil {
			ctx.Error(fmt.Sprintf("[cron::%s] task found error", route.Name), err, nil)
		}
	}
}

func NewCrontab(cronName string, log x.ILogger, routes ICrontabRoutes, opts ...cron.Option) *Crontab {
	return &Crontab{
		name:   cronName,
		cron:   cron.New(opts...),
		log:    log,
		routes: routes,
	}
}

type Crontab struct {
	name   string
	log    x.ILogger
	cron   *cron.Cron
	routes ICrontabRoutes
}

func (c *Crontab) Shutdown(ctx context.Context) error {
	c.cron.Stop()
	return nil
}
func (c *Crontab) TakeName() string {
	return fmt.Sprintf("crontab-%s", c.name)
}
func (c *Crontab) Boot() (err error) {
	c.bind()
	c.cron.Start()
	return
}

func (c *Crontab) bind() {
	jobIdMapName := make(map[cron.EntryID]string)

	for c.routes.HasNext() {
		route := c.routes.Next()
		entryId, err := c.cron.AddFunc(route.Spec, route.makeCronHandler(c.name, route, c.log))
		if err != nil {
			panic(fmt.Sprintf("route %s bind failed", c.name))
		}
		jobIdMapName[entryId] = route.Name
	}

	c.log.Info("cron 记录 entityId 到 entityIdMap", x.H{
		"cron_name": c.name,
		"job_map":   jobIdMapName,
	})
}
