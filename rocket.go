package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
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

type MessageQueueHandler func(ctx x.Context, message *mq.MessageView) error

type IMessageQueueRoutes interface {
	Register(name string, handle MessageQueueHandler)
	Take(name string) (handle MessageQueueHandler)
}

type messageQueueRoutes struct {
	routes map[string]MessageQueueHandler
}

func NewMessageQueueRoutes() IMessageQueueRoutes {
	return &messageQueueRoutes{
		routes: make(map[string]MessageQueueHandler),
	}
}

func (r messageQueueRoutes) Register(name string, handle MessageQueueHandler) {
	r.routes[name] = handle
}

func (r messageQueueRoutes) Take(name string) (handle MessageQueueHandler) {
	var (
		exists bool
	)

	handle, exists = r.routes[name]
	if !exists {
		panic(errors.New(fmt.Sprintf("route %s not found", name)))
	}

	return
}

type IMQ interface {
	Register(name string, handle MessageQueueHandler)
	Boot() error
}

type IMessageQueueOptions []IMessageQueueOption
type IMessageQueueOption interface {
	rocket.IOption
	TakeNum() int
	TakeWait() time.Duration
	TakeMaxMessageNum() int32
	TakeInvisibleDuration() time.Duration
	ErrorRetry() bool
}

type MessageQueueConsumerHandler func(topic, group string, opt rocket.IOption) rocket.IConsumer

func NewRocket(name string, log *logrus.Logger, registry IMessageQueueRoutes, options ...IMessageQueueOption) IService {
	return &defaultRocket{
		name:      name,
		options:   options,
		registry:  registry,
		log:       x.NewLogger(log),
		logSource: log,
	}
}

type defaultRocket struct {
	name      string
	options   IMessageQueueOptions
	registry  IMessageQueueRoutes
	log       x.ILogger
	logSource *logrus.Logger
}

func (s *defaultRocket) TakeName() string {
	return s.name
}

func (s *defaultRocket) Shutdown(ctx context.Context) error {
	return nil
}

func (s *defaultRocket) checkLog() {
	if s.log == nil {
		panic("mq service must has x.ILogger log Logger")
	}
}

func (s *defaultRocket) Listen(opt IMessageQueueOption) {
	var (
		err          error
		messageViews []*mq.MessageView
		consumer     mq.SimpleConsumer
	)

	pr.Yellow("rocket listen: %+v\n", opt)
	s.log.Info("[mq service] consumer::receive starting...", x.H{
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

func (s *defaultRocket) Accept(consumer mq.SimpleConsumer, messageViews []*mq.MessageView, opt IMessageQueueOption) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("[mq service] consumer::receive message handler panic", r.(error), x.H{
				"topic": opt.TakeTopic(),
				"group": opt.TakeGroup(),
			})
		}

	}()
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

		handler := s.registry.Take(*name)
		ctx := s.makeContext(messageView)
		ctx.Info("[mq service] consumer::receive message handler accept", x.H{
			"topic":  opt.TakeTopic(),
			"group":  opt.TakeGroup(),
			"tag":    name,
			"data":   string(messageView.GetBody()),
			"msg_id": messageView.GetMessageId(),
		})

		if err = handler(ctx, messageView); err != nil {
			ctx.Error("[mq service] consumer::receive message handler failed", err, x.H{
				"topic":  opt.TakeTopic(),
				"group":  opt.TakeGroup(),
				"tag":    name,
				"err":    err,
				"data":   string(messageView.GetBody()),
				"msg_id": messageView.GetMessageId(),
			})

			if !opt.ErrorRetry() {
				_ = consumer.Ack(context.Background(), messageView)
			}
			continue
		}

		ctx.Info("[mq service] consumer::receive message handler finish\n", x.H{
			"topic": opt.TakeTopic(),
			"group": opt.TakeGroup(),
			"tag":   name,
		})

		_ = consumer.Ack(context.Background(), messageView)
	}
}

func (s *defaultRocket) makeContext(message *mq.MessageView) (ctx x.Context) {
	body := message.GetBody()
	var js = struct {
		Context *x.ContextData `json:"context"`
	}{}

	_ = json.Unmarshal(body, &js)
	ctx = x.NewContextWithLog(s.logSource)
	ctx.GiveContextData(js.Context)
	return
}

func (s *defaultRocket) bootConsumer(option IMessageQueueOption) {
	pr.Yellow("rocket bootConsumer: %+v\n", option)
	for i := 0; i < option.TakeNum(); i++ {
		go s.Listen(option)
	}
}

func (s *defaultRocket) Boot() error {

	for _, option := range s.options {
		pr.Yellow("rocket boot: %+v\n", option)
		s.bootConsumer(option)
	}

	return nil
}

type Producer struct {
	opt    IMessageQueueOption
	source mq.Producer
}

func NewMessage(body []byte) *mq.Message {
	return &mq.Message{Body: body}
}

func NewProducer(opt IMessageQueueOption) *Producer {
	return &Producer{opt: opt}
}

func (c *Producer) makeOptions() []mq.ProducerOption {
	return []mq.ProducerOption{
		mq.WithTopics(c.opt.TakeTopic()),
	}
}

func (c *Producer) Make() (product *Producer, err error) {
	if c.source == nil {
		config := &mq.Config{
			Endpoint: c.opt.TakeEndpoint(),
			Credentials: &credentials.SessionCredentials{
				AccessKey:    c.opt.TakeAccessKey(),
				AccessSecret: c.opt.TakeSecretKey(),
			},
		}

		config.ConsumerGroup = c.opt.TakeGroup()
		if c.source, err = mq.NewProducer(config, c.makeOptions()...); err != nil {
			return
		}
	}

	return c, c.source.Start()
}

func (c *Producer) Stop() {
	_ = c.source.GracefulStop()
}

func (c *Producer) Push(ctx x.Context, message *mq.Message) ([]*mq.SendReceipt, error) {
	if _, err := c.Make(); err != nil {
		return nil, err
	}

	if message.Topic == "" {
		message.Topic = c.opt.TakeTopic()
	}

	return c.source.Send(ctx.TakeContext(), message)
}
