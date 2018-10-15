package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type mockRabbit struct {
	isOk bool
}

func (r mockRabbit) send(data []byte) error {
	if r.isOk {
		return nil
	}
	return errors.New("mock error")
}

func TestHandler(t *testing.T) {
	var rab mockRabbit
	rab.isOk = true

	p := &profile{
		Name: "name",
		URL:  "url"}
	js, err := json.Marshal(p)
	if err != nil {
		t.Errorf("Ошибка маршалинга структуры: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://localhost:8080/put", bytes.NewReader(js))
	handler(rab).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("Ошибка корректного метода. Код: %v", w.Code)
	}

	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", "http://localhost:8080/put", bytes.NewReader(js))
	handler(rab).ServeHTTP(w, r)
	if w.Code != http.StatusNotImplemented {
		t.Errorf("Ошибка некорректного метода. Код: %v", w.Code)
	}

	rab.isOk = false

	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", "http://localhost:8080/put", bytes.NewReader(js))
	handler(rab).ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Ошибка нерабочего соединения rabbitMQ. Код: %v", w.Code)
	}

	rab.isOk = true

	js = []byte("42")
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", "http://localhost:8080/put", bytes.NewReader(js))
	handler(rab).ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Ошибка некорректных входных данных. Код: %v", w.Code)
	}
}

func Test_handler(t *testing.T) {
	type args struct {
		rab sender
	}
	tests := []struct {
		name string
		args args
		want http.HandlerFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handler(tt.args.rab); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handler() = %v, want %v", got, tt.want)
			}
		})
	}
}
