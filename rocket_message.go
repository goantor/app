package app

import (
	"encoding/json"
	mq "github.com/apache/rocketmq-clients/golang"
	"github.com/goantor/pr"
	"github.com/goantor/x"
	"github.com/sirupsen/logrus"
	"time"
)

type TaskRouteName string

func (t TaskRouteName) String() string {
	return string(t)
}

// TaskAction 定义任务执行的动作
type TaskAction int

func (a TaskAction) Int() int {
	return int(a)
}

type TaskHeader struct {
	Context   x.IContextData `json:"context"`
	Name      TaskRouteName  `json:"name"`
	Action    TaskAction     `json:"action"`
	SendTime  time.Time      `json:"send_time"`
	DelayTime time.Time      `json:"delay_time"`
}

func NewTaskHeader(ctx x.Context, name TaskRouteName, action TaskAction) *TaskHeader {
	now := time.Now()
	return &TaskHeader{Context: ctx.TakeContextData(), Name: name, Action: action, SendTime: now, DelayTime: now}
}

type SendMessage struct {
	Header *TaskHeader `json:"header"`
	Body   string      `json:"body"`
}

func NewSendMessage(ctx x.Context, name TaskRouteName, action TaskAction) *SendMessage {
	return &SendMessage{
		Header: NewTaskHeader(ctx, name, action),
	}
}

func (s *SendMessage) GiveBody(val interface{}) (err error) {
	var body []byte
	if body, err = json.Marshal(val); err == nil {
		s.Body = string(body)
	}

	return
}

// DelayDuration 当前时间累加多少时间
func (s *SendMessage) DelayDuration(duration time.Duration) {
	s.Header.DelayTime = time.Now().Add(duration)
}

// DelayTime 精确的日期时间
func (s *SendMessage) DelayTime(ts time.Time) {
	s.Header.DelayTime = ts
}

func (s *SendMessage) Make() (message *mq.Message) {
	message = &mq.Message{}
	message.SetTag(s.Header.Name.String())

	js, _ := json.Marshal(s)
	message.Body = js

	if !s.Header.DelayTime.Equal(s.Header.SendTime) {
		message.SetDelayTimestamp(s.Header.DelayTime)
	}

	return
}

//func MakeRocketMessage()

type IMessage interface {
	TakeView() *mq.MessageView
	takeContext() x.Context
	Input(key string, value interface{}) interface{}
	BindJson(params interface{}) error
	Inputs() x.H
	TakeHeader() *TaskHeader
}

func NewRocketMessage(messageView *mq.MessageView, log *logrus.Logger) *RocketMessage {
	return &RocketMessage{messageView: messageView, log: log, data: x.H{}}
}

type RocketMessage struct {
	log *logrus.Logger

	messageView *mq.MessageView

	SendMessage *SendMessage

	data x.H

	ctx x.Context
}

func (r *RocketMessage) TakeView() *mq.MessageView {
	return r.messageView
}

func (r *RocketMessage) TakeHeader() *TaskHeader {
	return r.SendMessage.Header
}

func (r *RocketMessage) Init() {
	body := r.messageView.GetBody()

	r.SendMessage = &SendMessage{
		Header: &TaskHeader{
			Context: x.NewContextData(),
		},
	}

	r.SendMessage.Header.Context.GiveParams(interface{}(nil))

	_ = json.Unmarshal(body, &r.SendMessage)
	_ = json.Unmarshal(body, &r.data)
	pr.Yellow("send message: %+v, header: %+v\n", r.SendMessage, r.SendMessage.Header)
}

func (r *RocketMessage) takeContext() x.Context {
	if r.ctx == nil {
		r.ctx = x.NewContextWithLog(r.log)
		r.ctx.GiveContextData(r.SendMessage.Header.Context)
	}

	return r.ctx
}

func (r *RocketMessage) Input(key string, def interface{}) interface{} {
	return r.data.Input(key, def)
}

func (r *RocketMessage) BindJson(param interface{}) (err error) {
	r.ctx.GiveMark("MQ_PARAMS_INIT")
	defer r.ctx.CleanMark()

	if err = json.Unmarshal([]byte(r.SendMessage.Body), &param); err != nil {
		// 这里可能会出现隐私情况 所以暂不处理
		r.ctx.Error("message_queue bind json params failed", err, x.H{
			"topic":  r.messageView.GetTopic(),
			"group":  r.messageView.GetTag(),
			"name":   r.SendMessage.Header.Name,
			"action": r.SendMessage.Header.Action,
		})

		return
	}

	return
}

func (r *RocketMessage) Inputs() x.H {
	return r.data
}
