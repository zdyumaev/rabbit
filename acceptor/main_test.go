package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
