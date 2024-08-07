package app

import (
	"context"
	"errors"
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/goantor/pr"
	"github.com/goantor/rocket"
	"github.com/goantor/x"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

func CloseRocketLog() {
	os.Setenv("mq.consoleAppender.enabled", "false")
	os.Setenv("rocketmq.client.logLevel", "error")

	mq.ResetLogger()
}

func InitRocketLog(configPath string) {
	os.Setenv(mq.CLIENT_LOG_ROOT, configPath)
	os.Setenv(mq.CLIENT_LOG_ROOT, configPath+"/mq/")
	mq.ResetLogger()
}

type RocketHandler func(ctx x.Context, message IMessage, msgId string) error

type IRocketRoutes interface {
	Register(name string, handle RocketHandler)
	Take(name string) (handle RocketHandler)
}

type rocketRoutes struct {
	routes map[string]RocketHandler
}

func NewRocketRoutes() IRocketRoutes {
	return &rocketRoutes{
		routes: make(map[string]RocketHandler),
	}
}

func (r rocketRoutes) Register(name string, handle RocketHandler) {
	r.routes[name] = handle
}

func (r rocketRoutes) Take(name string) (handle RocketHandler) {
	var (
		exists bool
	)

	handle, exists = r.routes[name]
	if !exists {
		panic(errors.New(fmt.Sprintf("route %s not found", name)))
	}

	return
}

type IRocketOptions []IRocketOption
type IRocketOption interface {
	rocket.IOption
	TakeNum() int
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
	ErrorRetry() bool
}

type RocketConsumerHandler func(topic, group string, opt rocket.IOption) rocket.IConsumer

type RocketRouteFunc func(route IRocketRoutes)

func NewRocket(opt rocket.IConsumerOption, route RocketRouteFunc, log *logrus.Logger) IService {
	return &DefaultRocket{
		option:    opt,
		registry:  NewRocketRoutes(),
		routeFunc: route,

		log:       x.NewLogger(log),
		logSource: log,
	}
}

func NewRocketServices(opt rocket.IConsumerOption, log *logrus.Logger) IService {
	return &DefaultRocket{
		option:    opt,
		registry:  NewRocketRoutes(),
		log:       x.NewLogger(log),
		logSource: log,
	}
}

type DefaultRocket struct {
	option    rocket.IConsumerOption
	registry  IRocketRoutes
	log       x.ILogger
	logSource *logrus.Logger
	routeFunc RocketRouteFunc
}

func (s *DefaultRocket) BindRoutes(routeFunc RocketRouteFunc) IService {
	s.routeFunc = routeFunc
	return s
}

func (s *DefaultRocket) TakeConsumer() mq.SimpleConsumer {
	return rocket.NewConsumer(s.option).Connect()
}

func (s *DefaultRocket) TakeName() string {
	return s.option.TakeTopic()
}

func (s *DefaultRocket) Shutdown(ctx context.Context) error {
	return nil
}

func (s *DefaultRocket) checkLog() {
	if s.log == nil {
		panic("mq service must has x.ILogger log Logger")
	}
}

func (s *DefaultRocket) Listen(opt rocket.IConsumerOption) {
	var (
		err          error
		messageViews []*mq.MessageView
		consumer     mq.SimpleConsumer
	)

	pr.Yellow("rocket listen: %+v\n", opt)
	s.log.Info("[mq service] consumer::receive starting...", x.H{
		"topic": opt.TakeTopic(),
	})

	consumer = s.TakeConsumer()

	for {
		messageViews, err = consumer.Receive(context.Background(), opt.TakeMaxMessageNum(), opt.TakeInvisibleDuration())
		if err != nil {
			var status *mq.ErrRpcStatus
			if errors.As(err, &status) {
				if status.GetCode() == 40401 {
					time.Sleep(2 * time.Second)
					continue
				}
			}

			s.log.Error("[mq service] consumer::receive failed...", err, x.H{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			})

			time.Sleep(2 * time.Second)
			continue
		}

		for _, view := range messageViews {
			go s.Accept(consumer, view, opt)
		}
	}
}

func (s *DefaultRocket) Accept(consumer mq.SimpleConsumer, messageView *mq.MessageView, opt rocket.IConsumerOption) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("[mq service] consumer::receive message handler panic", r.(error), x.H{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			})

			_ = consumer.Ack(context.Background(), messageView)
		}
	}()

	var err error
	name := messageView.GetTag()
	if *name == "" {
		s.log.Error("[mq service] consumer::receive message without tag", errors.New("mq tag is nil"), x.H{
			"topic": opt.TakeTopic(),
			"group": opt.TakeGroup(),
		})
		pr.Red("not found name\n")
		_ = consumer.Ack(context.Background(), messageView)
		return
	}

	message := NewRocketMessage(messageView, s.logSource)
	message.Init()

	pr.Green("message: %+v\n", message)
	ctx := message.takeContext()

	handler := s.registry.Take(*name)
	if handler == nil {
		// 不处理了
		return
	}

	s.log.Info("[mq service] consumer::receive message handler accept", x.H{
		"topic":  opt.TakeTopic(),
		"group":  opt.TakeGroup(),
		"tag":    name,
		"msg_id": messageView.GetMessageId(),
	})

	if err = handler(ctx, message, messageView.GetMessageId()); err != nil {
		s.log.Error("[mq service] consumer::receive message handler failed", err, x.H{
			"topic":  opt.TakeTopic(),
			"group":  opt.TakeGroup(),
			"tag":    name,
			"err":    err,
			"msg_id": messageView.GetMessageId(),
		})

		_ = consumer.Ack(context.Background(), messageView)
		return
	}

	s.log.Info("[mq service] consumer::receive message handler finish\n", x.H{
		"topic": opt.TakeTopic(),
		"group": opt.TakeGroup(),
		"tag":   name,
	})

	_ = consumer.Ack(context.Background(), messageView)
	return
}

func (s *DefaultRocket) bootConsumer(option rocket.IConsumerOption) {
	pr.Yellow("boot opt: %+v\n", option)
	for i := 0; i < option.TakeNum(); i++ {
		go s.Listen(option)
	}
}

func (s *DefaultRocket) Boot() error {
	s.routeFunc(s.registry)
	fmt.Printf("rocket start\n\n")
	s.bootConsumer(s.option)

	return nil
}

func NewProducer(option rocket.IProducerOption) mq.Producer {
	return rocket.NewProducer(option).Connect()
}
