package app

import (
	"context"
	"encoding/json"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/goantor/rocket"
	"github.com/goantor/x"
	"github.com/sirupsen/logrus"
	"time"
)

type TaskName string
type MQHandler func(ctx x.Context, body []byte, message *mq.MessageView) error

type MQHandleRegistry map[TaskName]MQHandler

func (r MQHandleRegistry) Register(name TaskName, handle MQHandler) {
	r[name] = handle
}

func (r MQHandleRegistry) Take(name TaskName) (handle MQHandler, exists bool) {
	handle, exists = r[name]
	return
}

type IMQService interface {
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

type MqServiceConsumer struct {
}

type MQConsumerHandler func(topic, group string, opt rocket.IOption) rocket.IConsumer

type MQService struct {
	handlers IMQConsumerOptions
	registry MQHandleRegistry
	log      *logrus.Entry
}

func (s MQService) checkLog() {
	if s.log == nil {
		panic("mq service must has logrus.Entry log entity")
	}
}

func (s MQService) Listen(consumer mq.SimpleConsumer, opt IMQConsumerOption) {
	var (
		err          error
		messageViews []*mq.MessageView
	)

	s.log.Infof("[mq service] consumer::receive starting...")
	for {

		messageViews, err = consumer.Receive(context.Background(), opt.TakeMaxMessageNum(), opt.TakeInvisibleDuration())
		if err != nil {
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

func (s MQService) Accept(consumer mq.SimpleConsumer, messageViews []*mq.MessageView, opt IMQConsumerOption) {
	var err error
	for _, messageView := range messageViews {
		name := messageView.GetTag()
		if *name == "" {
			s.log.WithFields(logrus.Fields{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			}).Error("[mq service] consumer::receive message without tag")

			_ = consumer.Ack(context.Background(), messageView)
			continue
		}

		if handle, exists := s.registry.Take(TaskName(*name)); exists {
			ctx, data := s.makeContext(messageView)

			ctx.Info("[mq service] consumer::receive message handle start", x.H{
				"topic":  opt.TakeTopic(),
				"group":  opt.TakeGroup(),
				"tag":    name,
				"data":   string(data),
				"msg_id": messageView.GetMessageId(),
			})

			if err = handle(ctx, data, messageView); err != nil {
				ctx.Error("[mq service] consumer::receive message handle failed", err, x.H{
					"topic":  opt.TakeTopic(),
					"group":  opt.TakeGroup(),
					"tag":    name,
					"err":    err,
					"data":   string(data),
					"msg_id": messageView.GetMessageId(),
				})
			} else {
				ctx.Info("[mq service] consumer::receive message handle finish", nil)
			}
		}

		_ = consumer.Ack(context.Background(), messageView)
	}
}

func (s MQService) makeContext(message *mq.MessageView) (ctx x.Context, data []byte) {
	body := message.GetBody()
	var js = struct {
		Context x.IContextData `json:"context"`
		Data    []byte         `json:"data"`
	}{}

	_ = json.Unmarshal(body, &js)
	log := x.NewLoggerWithData(s.log, js.Context)
	return x.NewContext(log), js.Data
}

func (s MQService) bootConsumer(handler IMQConsumerOption) {
	for i := 0; i < 2; i++ {
		go s.Listen(
			rocket.NewConsumer(handler.(rocket.IOption)).Connect(handler.TakeWait()),
			handler,
		)
	}
}

func (s MQService) Boot() {
	for _, handler := range s.handlers {
		for i := 0; i < handler.TakeNum(); i++ {
			s.bootConsumer(handler)
		}
	}
}
