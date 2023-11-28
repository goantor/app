package app

import (
	"context"
	"fmt"
	"github.com/goantor/x"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"time"
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

type CrontabKind int

type TaskHandler func(ctx x.Context) error

type ICrontabRoutes interface {
	RegisterCron(name string, handler TaskHandler, spec string)
	RegisterLoop(name string, handler TaskHandler, interval time.Duration)
	Take(name string) (task *CrontabRoute)
	bind(cron *cron.Cron, log *logrus.Logger)
	Valid() bool
	RunLoop(log *logrus.Logger)
}

func NewCrontabRoutes() ICrontabRoutes {
	return &CrontabRoutes{
		routes: make(map[string]*CrontabRoute, 0),
		loops:  make(map[string]*LoopRoute, 0),
	}
}

type CrontabRoutes struct {
	routes map[string]*CrontabRoute
	loops  map[string]*LoopRoute
}

func (o *CrontabRoutes) Valid() bool {
	return len(o.routes) > 0
}

func (o *CrontabRoutes) bind(cron *cron.Cron, log *logrus.Logger) {
	for name, route := range o.routes {
		if _, err := cron.AddFunc(route.Spec, o.makeCronHandler(name, route, log)); err != nil {
			panic(fmt.Sprintf("route %s bind failed", name))
		}
	}
}

func (o *CrontabRoutes) makeCronHandler(name string, route *CrontabRoute, log *logrus.Logger) (handle func()) {
	return func() {
		ctx := x.NewContextWithLog(log)
		ctx.GiveService("crontab")
		ctx.GiveModule(name)
		if err := route.Handler(ctx); err != nil {
			ctx.Error(fmt.Sprintf("[cron::%s] task found error", route.Name), err, nil)
		}
	}
}

func (o *CrontabRoutes) Take(name string) (task *CrontabRoute) {
	var exists bool

	if task, exists = o.routes[name]; !exists {
		panic(fmt.Sprintf("task %s not found", name))
	}

	return
}

func (o *CrontabRoutes) RegisterCron(name string, handler TaskHandler, spec string) {
	o.routes[name] = &CrontabRoute{
		Name:    name,
		Handler: handler,
		Spec:    spec,
	}
}

func (o *CrontabRoutes) RegisterLoop(name string, handler TaskHandler, interval time.Duration) {
	o.loops[name] = &LoopRoute{
		Name:     name,
		Handler:  handler,
		Interval: interval,
	}
}

func (o *CrontabRoutes) RunLoop(log *logrus.Logger) {
	for _, route := range o.loops {
		go route.Run(log)
	}
}

type CrontabRoute struct {
	Name    string
	Handler TaskHandler
	Spec    string
}

type LoopRoute struct {
	Name     string
	Handler  TaskHandler
	Interval time.Duration
}

func (r LoopRoute) Run(log *logrus.Logger) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(fmt.Sprintf("[loop::%s] task found error", r.Name), err, nil)
		}

		r.Run(log)
	}()

	for {
		ctx := x.NewContextWithLog(log)
		ctx.GiveService("Loop")
		ctx.GiveModule(r.Name)
		if err := r.Handler(ctx); err != nil {
			ctx.Error(fmt.Sprintf("[loop::%s] task found error", r.Name), err, nil)
		}
		time.Sleep(r.Interval)
	}
}

func NewCrontab(opt cron.Option, log *logrus.Logger, routes ICrontabRoutes) *Crontab {
	return &Crontab{
		cron:   cron.New(opt),
		log:    log,
		routes: routes,
	}
}

type Crontab struct {
	log    *logrus.Logger
	cron   *cron.Cron
	routes ICrontabRoutes
}

func (c *Crontab) Shutdown(ctx context.Context) error {
	c.cron.Stop()
	return nil
}

func (c *Crontab) TakeName() string {
	return "crontab"
}

func (c *Crontab) RegisterCron(name string, handler TaskHandler, spec string) {
	c.routes.RegisterCron(name, handler, spec)
}

func (c *Crontab) RegisterLoop(name string, handler TaskHandler, interval time.Duration) {
	c.routes.RegisterLoop(name, handler, interval)
}

func (c *Crontab) Registers(routes ICrontabRoutes) {
	c.routes = routes
}

func (c *Crontab) Boot() (err error) {
	c.routes.bind(c.cron, c.log)
	c.cron.Start()

	c.routes.RunLoop(c.log)
	return
}
