package main

import (
	"encoding/json"
	"flag"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"../rabbit"
)

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

func main() {
	flag.Parse()

	db, err := sqlx.Open("postgres", *postgresURI)
	if err != nil {
		log.Printf("Ошибка соединения с базой данных: %v", err)
	}

	rabbit := rabbit.NewRabbit(*rabbitURI, *queueName, profileHandler(db))
	defer rabbit.Close()

	rabbit.Maintain()
}

const query = `INSERT INTO profile (name, url) VALUES
					 ($1, $2)`

// profileHandler добавляет приходящие данные в БД
func profileHandler(db *sqlx.DB) rabbit.HandleFunc {
	return func(data []byte) error {
		p := &profile{}
		// Дополнительная проверка на корректность
		err := json.Unmarshal(data, p)
		if err != nil {
			log.Printf("Ошибка разбора сообщения: %v. Данные: %v", err, string(data))
			return nil // Чтобы отбросить, так как возвращать в очередь некорректные данные бесполезно
		}
		_, err = db.Exec(query, p.Name, p.URL)
		return err
	}
}
