package hafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/kamva/hexa"
)

// logger implements sarama StdLogger
type logger struct {
	l hexa.Logger
}

func (l *logger) Print(v ...interface{}) {
	l.l.Info(fmt.Sprint(v...))
}

func (l *logger) Printf(format string, v ...interface{}) {
	l.l.Info(fmt.Sprintf(format, v...))
}

func (l *logger) Println(v ...interface{}) {
	l.l.Info(fmt.Sprintln(v...))
}

func NewLogger(l hexa.Logger) sarama.StdLogger {
	return &logger{l: l}
}

var _ sarama.StdLogger = &logger{}
