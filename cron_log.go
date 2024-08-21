package app

import (
	"github.com/goantor/x"
)

func NewCronLog(log x.ILogger) *CronLog {
	return &CronLog{
		log: log,
	}
}

type CronLog struct {
	log x.ILogger
}

func (cl *CronLog) Info(msg string, keysAndValues ...interface{}) {
	cl.log.Info(msg, x.H{
		"data": keysAndValues,
	})
}

func (cl *CronLog) Error(err error, msg string, keysAndValues ...interface{}) {
	cl.log.Error(msg, err, x.H{
		"data": keysAndValues,
	})
}
