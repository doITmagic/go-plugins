package rabbitmq

//
// All credit to Mondo
//

import (
	"errors"

	"github.com/nu7hatch/gouuid"
	"github.com/streadway/amqp"
)

type rabbitMQChannel struct {
	uuid       string
	connection *amqp.Connection
	channel    *amqp.Channel
}

func newRabbitChannel(conn *amqp.Connection) (*rabbitMQChannel, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	rabbitCh := &rabbitMQChannel{
		uuid:       id.String(),
		connection: conn,
	}
	if err := rabbitCh.Connect(); err != nil {
		return nil, err
	}
	return rabbitCh, nil

}

func (r *rabbitMQChannel) Connect() error {
	var err error
	r.channel, err = r.connection.Channel()
	if err != nil {
		return err
	}
	return nil
}

func (r *rabbitMQChannel) Close() error {
	if r.channel == nil {
		return errors.New("Channel is nil")
	}
	return r.channel.Close()
}

func (r *rabbitMQChannel) Publish(exchange, key string, message amqp.Publishing) error {
	if r.channel == nil {
		return errors.New("Channel is nil")
	}
	return r.channel.Publish(exchange, key, false, false, message)
}

func (r *rabbitMQChannel) DeclareExchange(exchange string,durable bool,autoDelete bool, internal bool,noWait bool) error {
	return r.channel.ExchangeDeclare(
		exchange, // name
		"topic",  // kind
		durable,    // durable
		autoDelete,    // autoDelete
		internal,    // internal
		noWait,    // noWait
		nil,      // args
	)
}

func (r *rabbitMQChannel) DeclareQueue(queue string,durable bool,autoDelete bool,exclusive bool,noWait bool) error {
	_, err := r.channel.QueueDeclare(
		queue, // name
		durable, // durable
		autoDelete,  // autoDelete
		exclusive, // exclusive
		noWait, // noWait
		nil,   // args
	)
	return err
}

func (r *rabbitMQChannel) DeclareDurableQueue(queue string,autoDelete bool,exclusive bool,noWait bool) error {
	_, err := r.channel.QueueDeclare(
		queue, // name
		true,  // durable
		autoDelete,  // autoDelete
		exclusive, // exclusive
		noWait, // noWait
		nil,   // args
	)
	return err
}

func (r *rabbitMQChannel) DeclareReplyQueue(queue string,durable bool,autoDelete bool,exclusive bool,noWait bool) error {
	_, err := r.channel.QueueDeclare(
		queue, // name
		durable, // durable
		autoDelete,  // autoDelete
		exclusive,  // exclusive
		noWait, // noWait
		nil,   // args
	)
	return err
}

func (r *rabbitMQChannel) ConsumeQueue(queue string, autoAck bool,exclusive bool, noLocal bool, noWait bool) (<-chan amqp.Delivery, error) {
	return r.channel.Consume(
		queue,   // queue
		r.uuid,  // consumer
		autoAck, // autoAck
		exclusive,   // exclusive
		noLocal,   // nolocal
		noWait,   // nowait
		nil,     // args
	)
}

func (r *rabbitMQChannel) BindQueue(queue, key, exchange string, args amqp.Table,noWait bool) error {
	return r.channel.QueueBind(
		queue,    // name
		key,      // key
		exchange, // exchange
		noWait,    // noWait
		args,     // args
	)
}
