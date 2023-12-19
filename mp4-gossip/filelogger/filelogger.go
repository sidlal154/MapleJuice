package filelogger

import (
	"fmt"
	"log"
	"mp4-gossip/config"
	"os"
)

/*
A custom logger wrapper using sirupsen logrus open library. This is useful for program debugging
*/
type FileLogger struct {
	cLogger *log.Logger
}

func NewFileLogger(config *config.Config) *FileLogger {
	logFileName := config.MachineName + ".log"
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("FILE NOT CREATED")
		return nil
	}
	logger := log.New(file, "", log.LstdFlags|log.Lmicroseconds)

	return &FileLogger{logger}
}

func (l *FileLogger) LogToFile(message string) {
	l.cLogger.Println(message)
}

func (l *FileLogger) Info(tag string, text string) {
	l.LogToFile(fmt.Sprintf("[%s]: %s", tag, text))
}

func (l *FileLogger) Warn(tag string, text string) {
	l.LogToFile(fmt.Sprintf("[%s]: %s", tag, text))
}

func (l *FileLogger) Debug(tag string, text string) {
	l.LogToFile(fmt.Sprintf("[%s]: %s", tag, text))
}

func (l *FileLogger) Error(tag string, text string, err error) {
	l.LogToFile(fmt.Sprintf("[%s]: %s : %s", tag, text, err))
}
