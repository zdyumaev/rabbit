package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const query = `INSERT INTO profile (name, url) VALUES
					 ($1, $2)`

var (
	rabbitURI   = flag.String("rabbitURI", "amqp://guest:guest@localhost:5672/", "rabbitMQ URI")
	queueName   = flag.String("queue", "profile", "Имя очереди")
	postgresURI = flag.String("postgresURI", "postgres://SMS:123456@192.168.1.38:5432/SMS?sslmode=disable", "postgreSQL URI")
)

// profile описывает структуру приходящих данных
type profile struct {
	Name string
	URL  string
}

// handleFunc объявляет тип обработчика приходящих сообщений
type handleFunc func([]byte) error

// deliveryHandler добавляет приходящие данные в БД
func deliveryHandler(db *sqlx.DB) handleFunc {
	return func(data []byte) error {
		p := &profile{}
		// Дополнительная проверка на корректность
		err := json.Unmarshal(data, p)
		if err != nil {
			log.Printf("Ошибка разбора сообщения: %v", err)
			return nil // Чтобы отбросить, так как возвращать в очередь некорректные данные бесполезно
		}
		_, err = db.Exec(query, p.Name, p.URL)

		all := make([]profile, 0)                        /////////////////////////
		db.Select(&all, "SELECT name, url FROM profile") /// добавлено для демонстрации работы БД
		fmt.Printf("%v\n", all)                          ///////////////////////////

		return err
	}
}

func main() {
	flag.Parse()

	db, err := sqlx.Open("postgres", *postgresURI)
	if err != nil {
		log.Printf("Ошибка соединения с базой данных: %v", err)
	}

	rab := newRabbit(*rabbitURI, *queueName, deliveryHandler(db))
	defer rab.close()

	forever := make(chan bool)
	<-forever
}
