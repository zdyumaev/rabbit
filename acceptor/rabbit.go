package main

import (
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// rabbit содержит все переменные для работы с rabbitMQ
type rabbit struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queue     *amqp.Queue
	errChan   chan *amqp.Error
	isClosed  bool
	uri       string
	queueName string
	isOK      bool
}

// rabbitService предоставляет интерфейс отпрвки данных
type rabbitService interface {
	send([]byte) error
}

// newRabbit инициализирует структуру
func newRabbit(uri, queueName string) *rabbit {
	r := &rabbit{
		uri:       uri,
		queueName: queueName}
	r.init()
	go r.reInit()
	return r
}

// send публикует сообщение, если соединение и канал готовы
func (r *rabbit) send(data []byte) error {
	if r.isOK {
		err := r.channel.Publish(
			"",
			r.queueName,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         data})
		return err
	}
	return errors.New("rabbit not ready")
}

// init подготавливает соединение с rabbitMQ и регистрирует очередь. Начинается заново при любой ошибке.
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
					// Инициализация структуры происходит только при отсутствии любых ошибок
					r.conn = conn
					r.channel = channel
					r.queue = &queue
					r.errChan = make(chan *amqp.Error)
					r.conn.NotifyClose(r.errChan)
					r.isOK = true
					log.Print("Соединение с rabbitMQ установлено")
					return
				}
				// Сбрасываем всё, чтобы далее повторить с начала при любой ошибке
				// Задержка, чтобы не дёргать постоянно в случае недоступности.  Можно удалить.
				log.Printf("Ошибка объявления очереди: %v", err)
				channel.Close()
				conn.Close()
				time.Sleep(time.Second)
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
			r.isOK = false
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
