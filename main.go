package main

import (
	"fmt"
	"go-rabbitmq/rabbitmq"
	"strconv"
	"time"
)

var (
	MQURL        string
	Durable      bool
	DeliveryMode uint8
)

func handlerTask(msg []byte) error {
	fmt.Println("消费：", string(msg))
	return nil
}

func main() {
	var (
		addr     = ""
		queue    = "hl-testQueue"
		exchange = "hl-test_exchange"
	)

	var Rabbit1 = rabbitmq.NewRabbit(addr, queue, exchange, true, false)

	for i := 1; i <= 3; i++ {
		consume := rabbitmq.NewConsumer(strconv.Itoa(i), addr, exchange, queue, false, handlerTask)
		if err := consume.Start(); err != nil {
			fmt.Println(err)
		}
	}

	for {
		time.Sleep(1 * time.Second)
		Rabbit1.PubMessage(time.Now().Format("2006-01-02 15:04:05"))
	}
}
