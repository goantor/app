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

func NewTaskRoutes() TaskRoutes {
	return make(TaskRoutes, 0)
}

type TaskRoutes []TaskRoute

func (o *TaskRoutes) Register(name string, handler TaskHandler, spec string) {
	*o = append(*o, TaskRoute{
		Name:    name,
		Handler: handler,
		Spec:    spec,
	})
}

type TaskRoute struct {
	Name    string
	Handler TaskHandler
	Spec    string
}

func NewCrontab(opt cron.Option, log *logrus.Logger, routes ...TaskRoute) *Crontab {
	return &Crontab{
		cron:   cron.New(opt),
		log:    log,
		routes: routes,
	}
}

type Crontab struct {
	log    *logrus.Logger
	cron   *cron.Cron
	routes TaskRoutes
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

func (c *Crontab) Registers(routes TaskRoutes) {
	if !c.valid() {
		c.routes = routes
		return
	}

	for _, route := range routes {
		c.routes.Register(route.Name, route.Handler, route.Spec)
	}
}

func (c *Crontab) makeHandler(route TaskRoute) (handle func()) {
	return func() {
		ctx := x.NewContextWithLog(c.log)
		if err := route.Handler(ctx); err != nil {
			ctx.Error(fmt.Sprintf("[cron::%s] task found error", route.Name), err, nil)
		}
	}
}

func (c *Crontab) registers() {
	for _, route := range c.routes {
		if _, err := c.cron.AddFunc(route.Spec, c.makeHandler(route)); err != nil {
			panic(err)
		}
	}
}

func (c *Crontab) valid() bool {
	return c.routes != nil && len(c.routes) > 0
}

func (c *Crontab) Boot() (err error) {
	if !c.valid() {
		return
	}

	c.registers()
	c.cron.Start()
	return
}
