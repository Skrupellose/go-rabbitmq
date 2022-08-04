package rabbitmq

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectDelay = 2 * time.Second // 连接断开后多久重连
	maxWaitNum     = 5               //最大等待次数
)

var (
	errNotConnected = errors.New("not connected to the producer")
)

type Rabbit struct {
	Addr        string
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	isConnected bool
	done        chan bool
	notifyClose chan *amqp.Error
	Queue       string
	Exchange    string
	Route       string
	Durable     bool
	AutoDelete  bool
}

//实体
func NewRabbit(addr string, queue string, exchange string, durable bool, autoDelete bool) *Rabbit {
	Rabbit := &Rabbit{
		done:       make(chan bool),
		Addr:       addr,
		Queue:      queue,
		Exchange:   exchange,
		Durable:    durable,
		AutoDelete: autoDelete,
	}
	// Rabbit.Addr = addr
	go Rabbit.handleReconnect()
	return Rabbit
}

func (r *Rabbit) handleReconnect() {
	for {
		if !r.isConnected || r.Connection.IsClosed() {
			log.Println("Amqp to connect")
			if ok, _ := r.Conn(r.Addr); !ok {
				log.Println("Failed to connect. Retrying...")
			}
		}
		time.Sleep(reconnectDelay)

		select {
		case <-r.done:
			return
		case <-r.notifyClose:
			r.Channel.Close()
			r.Connection.Close()
			r.isConnected = false
		}
	}
}

func (r *Rabbit) Conn(addr string) (bool, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return false, err
	}
	r.Connection = conn
	channel, err := conn.Channel()
	if err != nil {
		return false, err
	}
	r.changeConnection(conn, channel)
	r.isConnected = true
	log.Println("Connected!")
	return true, nil
}

func (r *Rabbit) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	r.Connection = connection
	r.Channel = channel

	r.notifyClose = make(chan *amqp.Error)
	r.Channel.NotifyClose(r.notifyClose)
}

//推送消息
func (r *Rabbit) PubMessage(msg string) (bool, error) {
	if ok, err := r.waitConn(); !ok {
		return ok, err
	}
	var (
		channel    = r.Channel
		err        error
		_Exchage   = r.Exchange
		_Queue     = r.Queue
		_Route     = r.Route
		durable    = r.Durable
		autoDelete = r.AutoDelete
	)

	err = channel.ExchangeDeclare(_Exchage, amqp.ExchangeFanout, durable, autoDelete, false, false, nil)
	if err != nil {
		return false, err
	}

	_, err = channel.QueueDeclare(_Queue, true, false, false, false, nil)
	if err != nil {
		return false, err
	}

	err = channel.QueueBind(_Queue, _Route, _Exchage, false, nil)

	if err != nil {
		return false, err
	}

	result := channel.Publish(_Exchage, "", false, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(msg),
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
	if result == nil {
		fmt.Println("push 成功：", msg)
		return true, nil
	} else {
		return false, result
	}
}

//等待重连
func (r *Rabbit) waitConn() (bool, error) {
	var _flag = false
	var i = 1
Loop:
	for {
		if i == maxWaitNum {
			break Loop
		}
		if _flag != r.isConnected {
			_flag = true
			break Loop
		}
		i++
		time.Sleep(1 * time.Second)
	}
	if !r.isConnected {
		return _flag, errNotConnected
	}
	return _flag, nil
}
