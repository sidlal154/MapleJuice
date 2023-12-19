package logger

import (
	log "github.com/sirupsen/logrus"
)

/*
A custom logger wrapper using sirupsen logrus open library. This is useful for program debugging
*/
type CustomLogger struct {
	cLogger *log.Logger
}

func NewLogger(level string) *CustomLogger {
	logger := log.New()
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	if level == "debug" {
		logger.SetLevel(log.DebugLevel)
	} else if level == "error" {
		logger.SetLevel(log.ErrorLevel)
	} else if level == "warn" {
		logger.SetLevel(log.WarnLevel)
	} else {
		logger.SetLevel(log.InfoLevel)
	}

	return &CustomLogger{logger}
}

func (l *CustomLogger) Info(tag string, text string) {
	l.cLogger.Info(tag + " : " + text)
}

func (l *CustomLogger) Warn(tag string, text string) {
	l.cLogger.Warn(tag + " : " + text)
}

func (l *CustomLogger) Debug(tag string, text string) {
	l.cLogger.Debug(tag + " : " + text)
}

func (l *CustomLogger) Error(tag string, text string, err error) {
	l.cLogger.Error(tag+" : "+text, err)
}
