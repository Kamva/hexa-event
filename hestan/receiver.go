// hestan (hexa stan) in implementation of Nats-streaming
// broker for hexa SDK using stan client library of NATS.
package hestan

import (
	"context"
	"encoding/json"
	"errors"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/kamva/gutil"
	"github.com/kamva/hexa"
	hevent "github.com/kamva/hexa-event"
	"github.com/kamva/hexa-event/internal/helper"
	"github.com/kamva/hexa/hlog"
	"github.com/kamva/tracer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"os"
	"syscall"
)

type handlerContext struct {
	context.Context
	msg *stan.Msg
}

type ReceiverOptions struct {
	NatsCon             *nats.Conn
	StreamingCon        stan.Conn
	CtxExporterImporter hexa.ContextExporterImporter
}

type receiver struct {
	cei hexa.ContextExporterImporter
	nc  *nats.Conn
	sc  stan.Conn
}

func (o ReceiverOptions) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.CtxExporterImporter, validation.Required),
		validation.Field(&o.NatsCon, validation.Required),
		validation.Field(&o.StreamingCon, validation.Required),
	)
}

func (h *handlerContext) Ack() {
	err := h.msg.Ack()
	if err != nil {
		hlog.Error("error in sending ack for msg",
			hlog.String("event_driver", "nats-streaming"),
			hlog.String("subject", h.msg.Subject),
			hlog.String("msg_string", h.msg.String()),
			hlog.String("error", err.Error()),
		)
	}
}

func (h *handlerContext) Nack() {
	// When we do not send ack, it assume nack.
}

func newHandlerCtx(msg *stan.Msg) hevent.HandlerContext {
	return &handlerContext{msg: msg}

}

func (r *receiver) Subscribe(channel string, p interface{}, h hevent.EventHandler) error {
	return r.SubscribeWithOptions(&hevent.SubscriptionOptions{
		Channel:         channel,
		PayloadInstance: p,
		Handler:         h,
	})
}

// SubscribeWithOptions subscribe with provided options.
// Note that nats-streaming does not support pattern for
// subscription subject name.
func (r *receiver) SubscribeWithOptions(o *hevent.SubscriptionOptions) error {
	if o.ChannelsPattern != "" || o.Channels != nil {
		errMsg := "nats-streaming driver does not support channel pattern and multi channel subscription"
		return tracer.Trace(errors.New(errMsg))
	}

	var options = &SubscriptionOptions{
		Subject:         o.Channel,
		Handler:         o.Handler,
		PayloadInstance: o.PayloadInstance,
	}

	// If provided native nats-streaming options, we
	// replace it with our options.
	for _, v := range o.Extra() {
		if nativeOptions, ok := v.(SubscriptionOptions); ok {
			options = &nativeOptions
		}
	}

	return r.subscribe(options)
}

func (r *receiver) subscribe(o *SubscriptionOptions) error {
	h := r.handler(o.PayloadInstance, o.Handler)
	opts := o.Opts
	if o.Position != nil {
		opts = append(opts, o.Position)
	}
	if o.Durable != "" {
		opts = append(opts, stan.DurableName(o.Durable))
	}

	hlog.Debug("subscribing to the subject",
		hlog.String("subject", o.Subject),
		hlog.String("group", o.Group),
		hlog.String("durable", o.Durable),
	)

	var err error
	if o.Group != "" {
		_, err = r.sc.QueueSubscribe(o.Subject, o.Group, h, opts...)
	} else {
		_, err = r.sc.Subscribe(o.Subject, h, opts...)
	}
	if err != nil {
		return tracer.Trace(err)
	}

	return nil
}

func (r *receiver) handler(p interface{}, h hevent.EventHandler) stan.MsgHandler {
	return func(msg *stan.Msg) {
		hlog.Debug("received event", hlog.String("subject", msg.Subject), hlog.String("msg", string(msg.Data)))
		ctx, m, err := r.extractMessage(msg.Data, p)
		h(newHandlerCtx(msg), ctx, m, tracer.Trace(err))
	}
}

func (r *receiver) extractMessage(msg []byte, payloadInstance interface{}) (ctx hexa.Context, m hevent.Message, err error) {
	rawMsg := hevent.RawMessage{}
	err = json.Unmarshal(msg, &rawMsg)
	if err != nil {
		err = tracer.Trace(err)
		return
	}

	// validate the message
	if err = rawMsg.Validate(); err != nil {
		err = tracer.Trace(err)
		return
	}
	// extract Context:
	ctx, err = r.cei.Import(rawMsg.MessageHeader.Ctx)
	if err != nil {
		err = tracer.Trace(err)
		return
	}
	m, err = helper.RawMessageToMessage(&rawMsg, payloadInstance)
	return
}

func (r *receiver) Start() error {
	return gutil.Wait(func(s os.Signal) error {
		return nil
	}, syscall.SIGINT, syscall.SIGTERM)
}

func (r *receiver) Close() error {
	defer r.nc.Close()
	return tracer.Trace(r.sc.Close())
}

// NewReceiver returns new instance of the Receiver
// using nats-streaming driver.
func NewReceiver(o ReceiverOptions) (hevent.Receiver, error) {
	return &receiver{
		cei: o.CtxExporterImporter,
		nc:  o.NatsCon,
		sc:  o.StreamingCon,
	}, o.Validate()
}

var _ hevent.Receiver = &receiver{}
