package rabbitmq

//
// All credit to Mondo
//

import (
	"crypto/tls"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	DefaultExchange  = "doitmagic"
	DefaultRabbitURL = "amqp://guest:guest@127.0.0.1:5672"

	dial    = amqp.Dial
	dialTLS = amqp.DialTLS
)

type rabbitMQConn struct {
	Connection      *amqp.Connection
	Channel         *rabbitMQChannel
	ExchangeChannel *rabbitMQChannel
	exchange        string
	url             string

	sync.Mutex
	connected bool
	close     chan bool
}

func NewRabbitMQConn(exchange string, urls []string) *rabbitMQConn {
	var url string

	if len(urls) > 0 && regexp.MustCompile("^amqp(s)?://.*").MatchString(urls[0]) {
		url = urls[0]
	} else {
		url = DefaultRabbitURL
	}

	if len(exchange) == 0 {
		exchange = DefaultExchange
	}

	return &rabbitMQConn{
		exchange: exchange,
		url:      url,
		close:    make(chan bool),
	}
}

func (r *rabbitMQConn) connect(secure bool, config *tls.Config,durable bool,autoDelete bool, internal bool,noWait bool) error {
	// try connect
	if err := r.tryConnect(secure, config,durable,autoDelete,internal,noWait); err != nil {
		return err
	}

	// connected
	r.Lock()
	r.connected = true
	r.Unlock()

	// create reconnect loop
	go r.reconnect(secure, config,durable,autoDelete,internal,noWait)
	return nil
}

func (r *rabbitMQConn) reconnect(secure bool, config *tls.Config, durable bool,autoDelete bool, internal bool,noWait bool) {
	// skip first connect
	var connect bool

	for {
		if connect {
			// try reconnect
			if err := r.tryConnect(secure, config,durable,autoDelete,internal,noWait); err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// connected
			r.Lock()
			r.connected = true
			r.Unlock()
		}

		connect = true
		notifyClose := make(chan *amqp.Error)
		r.Connection.NotifyClose(notifyClose)

		// block until closed
		select {
		case <-notifyClose:
			r.Lock()
			r.connected = false
			r.Unlock()
		case <-r.close:
			return
		}
	}
}

func (r *rabbitMQConn) Connect(secure bool, config *tls.Config, durable bool,autoDelete bool, internal bool,noWait bool) error {
	r.Lock()

	// already connected
	if r.connected {
		r.Unlock()
		return nil
	}

	// check it was closed
	select {
	case <-r.close:
		r.close = make(chan bool)
	default:
		// no op
		// new conn
	}

	r.Unlock()

	return r.connect(secure, config,durable,autoDelete,internal,noWait)
}

func (r *rabbitMQConn) Close() error {
	r.Lock()
	defer r.Unlock()

	select {
	case <-r.close:
		return nil
	default:
		close(r.close)
		r.connected = false
	}

	return r.Connection.Close()
}

func (r *rabbitMQConn) tryConnect(secure bool, config *tls.Config, durable bool,autoDelete bool, internal bool,noWait bool) error {
	var err error

	if secure || config != nil || strings.HasPrefix(r.url, "amqps://") {
		if config == nil {
			config = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		url := strings.Replace(r.url, "amqp://", "amqps://", 1)
		r.Connection, err = dialTLS(url, config)
	} else {
		r.Connection, err = dial(r.url)
	}

	if err != nil {
		return err
	}

	if r.Channel, err = newRabbitChannel(r.Connection); err != nil {
		return err
	}

	r.Channel.DeclareExchange(r.exchange,durable,autoDelete,internal,noWait)
	r.ExchangeChannel, err = newRabbitChannel(r.Connection)

	return err
}

func (r *rabbitMQConn) Consume(queue, key string, headers amqp.Table, autoAck, durableQueue bool,autoDelete bool,exclusive bool, noLocal bool, noWait bool) (*rabbitMQChannel, <-chan amqp.Delivery, error) {
	consumerChannel, err := newRabbitChannel(r.Connection)
	if err != nil {
		return nil, nil, err
	}

	if durableQueue {
		err = consumerChannel.DeclareDurableQueue(queue,autoDelete,exclusive,noWait)
	} else {
		err = consumerChannel.DeclareQueue(queue,durableQueue,autoDelete,exclusive,noWait)
	}

	if err != nil {
		return nil, nil, err
	}

	deliveries, err := consumerChannel.ConsumeQueue(queue, autoAck,exclusive,noLocal,noWait)
	if err != nil {
		return nil, nil, err
	}

	err = consumerChannel.BindQueue(queue, key, r.exchange, headers,noWait)
	if err != nil {
		return nil, nil, err
	}

	return consumerChannel, deliveries, nil
}

func (r *rabbitMQConn) Publish(exchange, key string, msg amqp.Publishing) error {
	return r.ExchangeChannel.Publish(exchange, key, msg)
}
