package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

// rabbit содержит все переменные для работы с rabbitMQ
type rabbit struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
	errChan    chan *amqp.Error
	isClosed   bool
	uri        string
	queueName  string
	deliveries <-chan amqp.Delivery
	handler    handleFunc
}

// newRabbit инициализирует структуру и регистрирует обработчик входящих сообщений
func newRabbit(uri, queueName string, f handleFunc) *rabbit {
	r := &rabbit{
		uri:       uri,
		queueName: queueName,
		handler:   f}

	r.init()
	go r.reInit()
	return r
}

// handle получает сообщения из очереди и вызывает обработчик, указанный при создании
func (r *rabbit) handle(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		data := d.Body
		err := r.handler(data)
		if err != nil {
			// Задержка, чтобы не дёргать постоянно в случае недоступности.  Можно удалить.
			time.Sleep(time.Second)

			d.Nack(false, true)
		} else {
			log.Print(string(data)) // Для наглядности выводим данные в случае удачного добавления в БД
			d.Ack(false)
		}
	}
}

// init подготавливает все поля структуры до полной готовности к приёму. Начинается заново при любой ошибке.
func (r *rabbit) init() {
	for {
		conn, err := amqp.Dial(r.uri)
		if err == nil {
			channel, err := conn.Channel()
			if err == nil {
				queue, err := channel.QueueDeclare(
					r.queueName,
					true, // durable
					false,
					false,
					false,
					nil)
				if err == nil {
					messages, err := channel.Consume(
						r.queueName, // queue
						"",          // messageConsumer
						false,       // auto-ack
						false,       // exclusive
						false,       // no-local
						false,       // no-wait
						nil)         // args
					if err == nil {
						r.conn = conn
						r.channel = channel
						r.queue = &queue
						r.deliveries = messages

						go r.handle(messages) // Установка обработчика приходящих сообщений

						r.errChan = make(chan *amqp.Error)
						r.conn.NotifyClose(r.errChan)
						log.Print("Соединение с rabbitMQ установлено")
						return
					}
					log.Printf("Ошибка регистрации потребителя: %v", err)
					// Сбрасываем всё, чтобы далее повторить с начала при любой ошибке
					// Задержка, чтобы не дёргать постоянно в случае недоступности.  Можно удалить.
					channel.Close()
					conn.Close()
					time.Sleep(time.Second)
				} else {
					log.Printf("Ошибка объявления очереди: %v", err)
					channel.Close()
					conn.Close()
					time.Sleep(time.Second)
				}
			} else {
				log.Printf("Ошибка создания канала rabbitMQ: %v", err)
				conn.Close()
				time.Sleep(time.Second)
			}
		} else {
			log.Printf("Ошибка подключения к rabbitMQ: %v", err)
			time.Sleep(time.Second)
		}
	}
}

// reInit пытается восстановить связь в случае закрытия соединения
func (r *rabbit) reInit() {
	for {
		err := <-r.errChan
		if !r.isClosed {
			log.Printf("Соединение с rabbitMQ закрыто: %v", err)
			r.init()
		}
	}
}

// close освобождает ресурсы
func (r *rabbit) close() {
	r.isClosed = true
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
