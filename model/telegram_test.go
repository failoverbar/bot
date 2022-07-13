package model

import (
	"context"
	"errors"
	"github.com/failoverbar/bot/wrap"
	"testing"
)

var tpr *TelegramProfileRepo

func TestTelegramProfile(t *testing.T) {
	tpr = &TelegramProfileRepo{DB: db}
	t.Run("create", testTelegramProfileCreateTable)
	t.Run("insert", testTelegramProfileInsert)
	t.Run("get", testTelegramProfileGet)
	t.Run("update", testTelegramProfileUpdate)
	t.Run("delete", testTelegramProfileDelete)
}

func testTelegramProfileCreateTable(t *testing.T) {
	if err := tpr.CreateTable(context.Background()); err != nil {
		t.Error(err)
	}
}

func testTelegramProfileInsert(t *testing.T) {
	u := &TelegramProfile{
		UserID: userID,
	}
	err := tpr.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}
}

func testTelegramProfileGet(t *testing.T) {
	u, err := tpr.Get(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	if u.UserID != userID {
		t.Error("wrong user id", u)
	}
}

func testTelegramProfileUpdate(t *testing.T) {
	u, err := tpr.Get(context.Background(), userID)
	if err != nil {
		t.Error("get: ", err)
	}
	u.FirstName = "test"
	err = tpr.Upsert(context.Background(), u)
	if err != nil {
		t.Error("upsert: ", err)
	}
	u, err = tpr.Get(context.Background(), userID)
	if err != nil {
		t.Error("get: ", err)
	}
	if u.FirstName != "test" {
		t.Error("nothing changed", u)
	}
}

func testTelegramProfileDelete(t *testing.T) {
	err := tpr.Delete(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	_, err = tpr.Get(context.Background(), userID)
	if !errors.Is(err, wrap.NotFoundError{}) {
		t.Error("not not_found error", err)
	}
}
