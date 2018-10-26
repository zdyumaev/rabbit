// Package rabbit - это обёртка над библиотекой AMQP для создания типа
// очереди rabbitMQ, которая позволяет принимать и отправлять сообщения,
// и восстанавливать соединение в случае ошибок.
// Имеет ограничения по настройке параметров очереди и обмена.
// Практично в рамках конкретной задачи, но слабо расширяемо.
package rabbit

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

// Publisher предоставляет интерфейс отправки данных
type Publisher interface {
	Publish([]byte) error
}

// HandleFunc объявляет тип обработчика приходящих сообщений
type HandleFunc func([]byte) error

// Queue содержит все переменные для работы с rabbitMQ
type Queue struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    *amqp.Queue
	errChan  chan *amqp.Error
	isClosed bool
	uri      string
	name     string
	handler  HandleFunc
	isOK     bool
}

// NewQueue возвращает инициализированное соединение с rabbitMQ.
// Блокирует рутину до удачной инициализации.
// Если обработчик f задан, то инициализируется подписка на очередь, иначе подписка не осуществляется.
func NewQueue(uri, queueName string, f HandleFunc) *Queue {
	r := &Queue{
		uri:     uri,
		name:    queueName,
		handler: f}
	r.init()
	return r
}

// Publish публикует сообщение, если соединение готово
func (q *Queue) Publish(data []byte) error {
	if q.isOK {
		err := q.channel.Publish(
			"",
			q.name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Body:         data})
		return err
	}
	return errors.New("queue not ready")
}

// Maintain пытается восстановить связь в случае закрытия соединения
func (q *Queue) Maintain() {
	for {
		err := <-q.errChan
		log.Printf("Соединение с rabbitMQ закрыто: %v", err)
		q.isOK = false
		if !q.isClosed {
			q.init()
		}
	}
}

// Close освобождает ресурсы
func (q *Queue) Close() {
	q.isClosed = true
	if q.channel != nil {
		q.channel.Close()
	}
	if q.conn != nil {
		q.conn.Close()
	}
}

// handle получает сообщения из очереди и вызывает обработчик, указанный при создании.
// Запускается отдельной рутиной.
func (q *Queue) handle(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		data := delivery.Body
		err := q.handler(data)
		if err != nil {
			delivery.Nack(false, true)
		} else {
			delivery.Ack(false)
		}
	}
}

// init подготавливает все поля структуры до полной готовности к приёму. Начинается заново при ошибке на любой стадии.
func (q *Queue) init() {
	for {
		conn, err := amqp.Dial(q.uri)
		if err != nil {
			log.Printf("Ошибка подключения к rabbitMQ: %v", err)
			continue
		}
		channel, err := conn.Channel()
		if err != nil {
			log.Printf("Ошибка создания канала rabbitMQ: %v", err)
			conn.Close()
			continue
		}
		queue, err := channel.QueueDeclare(
			q.name,
			true, // durable
			false,
			false,
			false,
			nil)
		if err != nil {
			log.Printf("Ошибка объявления очереди: %v", err)
			channel.Close()
			conn.Close()
			continue
		}
		// Если обработчик не установлен, то инициализация закончена
		if q.handler == nil {
			q.setFields(conn, channel, &queue)
			return
		}
		// Иначе - подписываемся
		messages, err := channel.Consume(
			q.name,
			"",
			false, // auto-ack
			false,
			false,
			false,
			nil)
		if err != nil {
			log.Printf("Ошибка регистрации потребителя: %v", err)
			channel.Close()
			conn.Close()
			continue
		}
		q.setFields(conn, channel, &queue)
		go q.handle(messages) // Установка обработчика приходящих сообщений
		return
	}
}

// initFields заполняет поля структуры после успешной инициализации и создаёт канал уведомлений о закрытии соединения
func (q *Queue) setFields(conn *amqp.Connection, channel *amqp.Channel, queue *amqp.Queue) {
	q.conn = conn
	q.channel = channel
	q.queue = queue
	q.errChan = make(chan *amqp.Error)
	q.conn.NotifyClose(q.errChan)
	q.isOK = true
	log.Print("Соединение с rabbitMQ установлено")
}
