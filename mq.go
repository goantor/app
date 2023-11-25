package app

import (
	"context"
	"encoding/json"
	"errors"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/goantor/rocket"
	"github.com/goantor/x"
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

type IMQOptions []IMQOption
type IMQOption interface {
	rocket.IOption
	TakeNum() int
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
}

type MQConsumerHandler func(topic, group string, opt rocket.IOption) rocket.IConsumer

func NewMQ(log x.ILogger, registry MQHandleRegistry, options ...IMQOption) IService {
	return &MQ{options: options, registry: registry, log: log}
}

type MQ struct {
	options  IMQOptions
	registry MQHandleRegistry
	log      x.ILogger
}

func (s MQ) TakeName() string {
	return "rocket"
}

func (s MQ) Shutdown(ctx context.Context) error {
	return nil
}

func (s MQ) checkLog() {
	if s.log == nil {
		panic("mq service must has logrus.Entry log entity")
	}
}

func (s MQ) Listen(opt IMQOption) {
	var (
		err          error
		messageViews []*mq.MessageView
		consumer     mq.SimpleConsumer
	)

	s.log.Info("[mq service] %s consumer::receive starting...", x.H{
		"topic": opt.TakeTopic(),
	})

	consumer = rocket.NewConsumer(opt.(rocket.IOption)).Connect(opt.TakeWait())

	for {
		messageViews, err = consumer.Receive(context.Background(), opt.TakeMaxMessageNum(), opt.TakeInvisibleDuration())
		if err != nil {
			var status *mq.ErrRpcStatus
			if errors.As(err, &status) {
				if status.GetCode() == 40401 {
					continue
				}
			}

			s.log.Error("[mq service] consumer::receive failed...", err, x.H{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			})

			continue
		}

		go s.Accept(consumer, messageViews, opt)
	}
}

func (s MQ) Accept(consumer mq.SimpleConsumer, messageViews []*mq.MessageView, opt IMQOption) {
	var err error
	for _, messageView := range messageViews {
		name := messageView.GetTag()
		if *name == "" {

			s.log.Error("[mq service] consumer::receive message without tag", errors.New("mq tag is nil"), x.H{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			})

			_ = consumer.Ack(context.Background(), messageView)
			continue
		}

		if handle, exists := s.registry.Take(TaskName(*name)); exists {
			ctx := s.makeContext(messageView)
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
				ctx.Info("[mq service] consumer::receive message handle finish\n", x.H{
					"topic": opt.TakeTopic(),
					"group": opt.TakeGroup(),
					"tag":   name,
				})
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
	ctx = x.NewContext(s.log)
	ctx.GiveContextData(js.Context)
	return
}

func (s MQ) bootConsumer(option IMQOption) {
	for i := 0; i <= option.TakeNum(); i++ {
		go s.Listen(

			option,
		)
	}
}

func (s MQ) Boot() error {
	for _, option := range s.options {
		s.bootConsumer(option)
	}

	return nil
}
