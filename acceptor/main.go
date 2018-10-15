package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"../rabbit"
)

var (
	rabbitURI = flag.String("rabbitURI", "amqp://guest:guest@localhost:5672/", "rabbitMQ URI")
	queueName = flag.String("queue", "profile", "Имя очереди")
	address   = flag.String("address", "localhost", "Адрес сервера")
	port      = flag.String("port", "8080", "Порт сервера")
)

// profile описывает структуру приходящих данных
type profile struct {
	Name string
	URL  string
}

func main() {
	flag.Parse()

	rabbit := rabbit.NewRabbit(*rabbitURI, *queueName, nil)
	defer rabbit.Close()

	go rabbit.Maintain()

	listenString := *address + ":" + *port
	log.Print("Запуск сервера: ", listenString)
	http.HandleFunc("/put", handler(rabbit))
	err := http.ListenAndServe(listenString, nil)

	if err != nil {
		log.Printf("Ошибка веб-сервера: %v", err)
	}
}

// handler обрабатывает входящие запросы
func handler(rabbit rabbit.Sender) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			log.Printf("Попытка запроса методом %v", r.Method)
			http.Error(w, "Метод не поддерживается", http.StatusNotImplemented)
			return
		}
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Ошибка чтения тела запроса: %v", err)
			http.Error(w, "Внутренняя ошибка", http.StatusInternalServerError)
			return
		}
		p := &profile{}
		// Проверка корректности входящих данных
		err = json.Unmarshal(data, p)
		if err != nil {
			log.Printf("Ошибка разбора запроса: %v", err)
			http.Error(w, "Ошибка в запросе", http.StatusBadRequest)
			return
		}
		err = rabbit.Send(data)
		if err != nil {
			log.Printf("Ошибка публикации сообщения: %v", err)
			http.Error(w, "Сервис недоступен", http.StatusServiceUnavailable)
		}
	}
}
