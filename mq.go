package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/goantor/rocket"
	"github.com/goantor/x"
	"github.com/sirupsen/logrus"
	"time"
)

type TaskName string
type MQHandler func(ctx x.Context, message *mq.MessageView) error

type MQHandleRegistry map[TaskName]MQHandler

func (r MQHandleRegistry) Register(name TaskName, handle MQHandler) {
	r[name] = handle
}

func (r MQHandleRegistry) Take(name TaskName) (handle MQHandler, exists bool) {
	handle, exists = r[name]
	return
}

type IMQ interface {
	Register(name TaskName, handle MQHandler)
	Boot() error
}

type IMQConsumerOptions []IMQConsumerOption
type IMQConsumerOption interface {
	rocket.IOption
	TakeNum() int
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
}

////type MqConsumer struct {
//}

type MQConsumerHandler func(topic, group string, opt rocket.IOption) rocket.IConsumer

type MQ struct {
	handlers IMQConsumerOptions
	registry MQHandleRegistry
	log      *logrus.Logger
}

func NewMQ(handlers IMQConsumerOptions, registry MQHandleRegistry, log *logrus.Logger) *MQ {
	return &MQ{handlers: handlers, registry: registry, log: log}
}

func (s MQ) checkLog() {
	if s.log == nil {
		panic("mq service must has logrus.Entry log entity")
	}
}

func (s MQ) Listen(consumer mq.SimpleConsumer, opt IMQConsumerOption) {
	var (
		err          error
		messageViews []*mq.MessageView
	)

	s.log.Infof("[mq service] %s consumer::receive starting...", opt.TakeTopic())
	for {
		messageViews, err = consumer.Receive(context.Background(), opt.TakeMaxMessageNum(), opt.TakeInvisibleDuration())
		if err != nil {
			var status *mq.ErrRpcStatus
			if errors.As(err, &status) {
				if status.GetCode() == 40401 {
					continue
				}
			}

			s.log.WithFields(logrus.Fields{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
				"err":   err,
			}).Error("[mq service] consumer::receive failed")
			continue
		}

		go s.Accept(consumer, messageViews, opt)
	}
}

func (s MQ) Accept(consumer mq.SimpleConsumer, messageViews []*mq.MessageView, opt IMQConsumerOption) {
	var err error
	for _, messageView := range messageViews {
		name := messageView.GetTag()
		fmt.Printf("start %s task\n", *name)
		if *name == "" {
			s.log.WithFields(logrus.Fields{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			}).Error("[mq service] consumer::receive message without tag")

			_ = consumer.Ack(context.Background(), messageView)
			continue
		}
		fmt.Printf("find %s task\n", *name)
		fmt.Printf("registry items: %+v\n", s.registry)
		if handle, exists := s.registry.Take(TaskName(*name)); exists {
			ctx := s.makeContext(messageView)
			fmt.Printf("do %s task\n", *name)
			ctx.Info("[mq service] consumer::receive message handle start", x.H{
				"topic":  opt.TakeTopic(),
				"group":  opt.TakeGroup(),
				"tag":    name,
				"data":   string(messageView.GetBody()),
				"msg_id": messageView.GetMessageId(),
			})

			if err = handle(ctx, messageView); err != nil {
				ctx.Error("[mq service] consumer::receive message handle failed", err, x.H{
					"topic":  opt.TakeTopic(),
					"group":  opt.TakeGroup(),
					"tag":    name,
					"err":    err,
					"data":   string(messageView.GetBody()),
					"msg_id": messageView.GetMessageId(),
				})
			} else {
				ctx.Info("[mq service] consumer::receive message handle finish\n", nil)
			}
		}

		_ = consumer.Ack(context.Background(), messageView)
	}
}

func (s MQ) makeContext(message *mq.MessageView) (ctx x.Context) {
	body := message.GetBody()
	var js = struct {
		Context *x.ContextData `json:"context"`
	}{}

	_ = json.Unmarshal(body, &js)
	log := x.NewLoggerWithData(s.log, js.Context)
	return x.NewContext(log)
}

func (s MQ) bootConsumer(handler IMQConsumerOption) {
	for i := 0; i <= handler.TakeNum(); i++ {
		go s.Listen(
			rocket.NewConsumer(handler.(rocket.IOption)).Connect(handler.TakeWait()),
			handler,
		)
	}
}

func (s MQ) Boot() {
	//os.
	fmt.Printf("do handlers: %d", len(s.handlers))
	for _, handler := range s.handlers {
		s.bootConsumer(handler)
	}
}
