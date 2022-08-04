package rabbitmq

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Consumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	connNotify    chan *amqp.Error
	channelNotify chan *amqp.Error
	quit          chan struct{}
	addr          string
	exchange      string
	queue         string
	routingKey    string
	consumerTag   string
	autoDelete    bool
	handler       func([]byte) error
}

func NewConsumer(name string, addr, exchange, queue string, autoDelete bool, handler func([]byte) error) *Consumer {
	c := &Consumer{
		addr:        addr,
		exchange:    exchange,
		queue:       queue,
		routingKey:  "",
		consumerTag: name,
		autoDelete:  autoDelete,
		handler:     handler,
		quit:        make(chan struct{}),
	}

	return c
}

func (c *Consumer) Start() error {
	if err := c.Run(); err != nil {
		fmt.Println("Run", err)
		return err
	}
	go c.ReConnect()

	return nil
}

func (c *Consumer) Stop() {
	close(c.quit)

	if !c.conn.IsClosed() {
		// 关闭 SubMsg message delivery
		if err := c.channel.Cancel(c.consumerTag, true); err != nil {
			log.Println("rabbitmq consumer - channel cancel failed: ", err)
		}

		if err := c.conn.Close(); err != nil {
			log.Println("rabbitmq consumer - connection close failed: ", err)
		}
	}
}

func (c *Consumer) Run() error {
	var err error
	if c.conn, err = amqp.Dial(c.addr); err != nil {
		fmt.Println("conn", err)
		return err
	}

	if c.channel, err = c.conn.Channel(); err != nil {
		fmt.Println("conn", err)
		c.conn.Close()
		return err
	}

	if _, err = c.channel.QueueDeclare(
		c.queue,      // name
		true,         // durable
		c.autoDelete, // delete when usused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	if err = c.channel.QueueBind(
		c.queue,
		c.routingKey,
		c.exchange,
		false,
		nil,
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	err = c.channel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		fmt.Printf("Qos设置失败：%v\n", err)
	}

	var delivery <-chan amqp.Delivery
	if delivery, err = c.channel.Consume(
		c.queue,       // queue
		c.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	go c.Handle(delivery)

	c.connNotify = c.conn.NotifyClose(make(chan *amqp.Error))
	c.channelNotify = c.channel.NotifyClose(make(chan *amqp.Error))

	return err
}

func (c *Consumer) ReConnect() {
	for {
		select {
		case err := <-c.connNotify:
			if err != nil {
				log.Println("rabbitmq consumer - connection NotifyClose: ", err)
			}
		case err := <-c.channelNotify:
			if err != nil {
				log.Println("rabbitmq consumer - channel NotifyClose: ", err)
			}
		case <-c.quit:
			return
		}

		// backstop
		if !c.conn.IsClosed() {
			// close message delivery
			if err := c.channel.Cancel(c.consumerTag, true); err != nil {
				log.Println("rabbitmq consumer - channel cancel failed: ", err)
			}

			if err := c.conn.Close(); err != nil {
				log.Println("rabbitmq consumer - channel cancel failed: ", err)
			}
		}

		// IMPORTANT: 必须清空 Notify，否则死连接不会释放
		for err := range c.channelNotify {
			println(err)
		}
		for err := range c.connNotify {
			println(err)
		}

	quit:
		for {
			select {
			case <-c.quit:
				return
			default:
				log.Printf("rabbitmq %v - reconnect", c.consumerTag)

				if err := c.Run(); err != nil {
					log.Println("rabbitmq consumer - failCheck: ", err)

					// sleep 5s reconnect
					time.Sleep(time.Second * 5)
					continue
				}

				break quit
			}
		}
	}
}

func (c *Consumer) Handle(delivery <-chan amqp.Delivery) {
	for d := range delivery {
		time.Sleep(3 * time.Second)
		go func(delivery amqp.Delivery) {
			if err := c.handler(delivery.Body); err == nil {
				fmt.Printf("from %v consume %v\n", c.consumerTag, string(delivery.Body))
				delivery.Ack(false)
			} else {
				// 重新入队，否则未确认的消息会持续占用内存
				delivery.Reject(true)
			}
		}(d)
	}
}
