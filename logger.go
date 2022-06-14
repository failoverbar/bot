package main

import (
	"encoding/json"
	"log"

	tele "gopkg.in/telebot.v3"
)

func Logger(logger ...*log.Logger) tele.MiddlewareFunc {
	var l *log.Logger
	if len(logger) > 0 {
		l = logger[0]
	} else {
		l = log.Default()
	}

	return func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			data, _ := json.MarshalIndent(c.Message(), "", "  ")
			l.Println("got telegram data: \n", string(data))
			return next(c)
		}
	}
}

func AutoResponder(next tele.HandlerFunc) tele.HandlerFunc {
	return func(c tele.Context) error {
		if c.Callback() != nil {
			log.Printf("add autocallback")
			defer c.Respond()
		}
		return next(c) // continue execution chain
	}
}
