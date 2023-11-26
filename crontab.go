package app

import (
	"context"
	"fmt"
	"github.com/goantor/x"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
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

type TaskHandler func(ctx x.Context) error

type ICrontabRoutes interface {
	Register(name string, handler TaskHandler, spec string)
	Take(name string) (task *CrontabRoute)
	bind(cron *cron.Cron, log *logrus.Logger)
	Valid() bool
}

func NewCrontabRoutes() ICrontabRoutes {
	return &CrontabRoutes{
		routes: make(map[string]*CrontabRoute, 0),
	}
}

type CrontabRoutes struct {
	routes map[string]*CrontabRoute
}

func (o *CrontabRoutes) Valid() bool {
	return len(o.routes) > 0
}

func (o *CrontabRoutes) bind(cron *cron.Cron, log *logrus.Logger) {
	for name, route := range o.routes {
		if _, err := cron.AddFunc(route.Spec, o.makeHandler(name, route, log)); err != nil {
			panic(fmt.Sprintf("route %s bind failed", name))
		}
	}
}

func (o *CrontabRoutes) makeHandler(name string, route *CrontabRoute, log *logrus.Logger) (handle func()) {
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

func (o *CrontabRoutes) Register(name string, handler TaskHandler, spec string) {
	o.routes[name] = &CrontabRoute{
		Name:    name,
		Handler: handler,
		Spec:    spec,
	}
}

type CrontabRoute struct {
	Name    string
	Handler TaskHandler
	Spec    string
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

func (c *Crontab) Register(name string, handler TaskHandler, spec string) {
	c.routes.Register(name, handler, spec)
}

func (c *Crontab) Registers(routes ICrontabRoutes) {
	c.routes = routes
}

func (c *Crontab) Boot() (err error) {
	c.routes.bind(c.cron, c.log)
	c.cron.Start()
	return
}
