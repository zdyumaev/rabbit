package rabbit

import (
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Sender предоставляет интерфейс отправки данных
type Sender interface {
	Send([]byte) error
}

// HandleFunc объявляет тип обработчика приходящих сообщений
type HandleFunc func([]byte) error

// Rabbit содержит все переменные для работы с rabbitMQ
type Rabbit struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queue     *amqp.Queue
	errChan   chan *amqp.Error
	isClosed  bool
	uri       string
	queueName string
	handler   HandleFunc
	isOK      bool
}

// NewRabbit возвращает инициализированное соединение с rabbitMQ.
// Блокирует рутину до удачной инициализации.
// Если обработчик f задан, то инициализируется подписка на очередь,
// если нет, то подписка не осуществляется.
func NewRabbit(uri, queueName string, f HandleFunc) *Rabbit {
	r := &Rabbit{
		uri:       uri,
		queueName: queueName,
		handler:   f}
	r.init()
	return r
}

// Send публикует сообщение, если соединение готово
func (r *Rabbit) Send(data []byte) error {
	if r.isOK {
		err := r.channel.Publish(
			"",
			r.queueName,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Body:         data})
		return err
	}
	return errors.New("rabbit not ready")
}

// Maintain пытается восстановить связь в случае закрытия соединения
func (r *Rabbit) Maintain() {
	for {
		err := <-r.errChan
		log.Printf("Соединение с rabbitMQ закрыто: %v", err)
		r.isOK = false
		if !r.isClosed {
			r.init()
		}
	}
}

// Close освобождает ресурсы
func (r *Rabbit) Close() {
	r.isClosed = true
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

// handle получает сообщения из очереди и вызывает обработчик, указанный при создании.
// Запускается отдельной рутиной.
func (r *Rabbit) handle(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		data := d.Body
		err := r.handler(data)
		if err != nil {
			d.Nack(false, true)
		} else {
			d.Ack(false)
		}
	}
}

// init подготавливает все поля структуры до полной готовности к приёму. Начинается заново при любой ошибке.
func (r *Rabbit) init() {
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
					if r.handler != nil {
						messages, err := channel.Consume(
							r.queueName,
							"",
							false, // auto-ack
							false,
							false,
							false,
							nil)
						if err == nil {
							r.initFields(conn, channel, &queue)
							go r.handle(messages) // Установка обработчика приходящих сообщений
							return
						}
					} else {
						r.initFields(conn, channel, &queue)
						return
					}
					log.Printf("Ошибка регистрации потребителя: %v", err)
					// Сбрасываем всё, чтобы далее повторить с начала при любой ошибке

				} else {
					log.Printf("Ошибка объявления очереди: %v", err)
				}
				channel.Close()
			} else {
				log.Printf("Ошибка создания канала rabbitMQ: %v", err)
			}
			conn.Close()
		} else {
			log.Printf("Ошибка подключения к rabbitMQ: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

// initFields заполняет поля структуры после успешной инициализации и создаёт канал уведомлений о закрытии соединения
func (r *Rabbit) initFields(conn *amqp.Connection, channel *amqp.Channel, queue *amqp.Queue) {
	r.conn = conn
	r.channel = channel
	r.queue = queue
	r.errChan = make(chan *amqp.Error)
	r.conn.NotifyClose(r.errChan)
	r.isOK = true
	log.Print("Соединение с rabbitMQ установлено")
}
