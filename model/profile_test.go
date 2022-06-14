package model

import (
	"context"
	"errors"
	"github.com/AlekSi/pointer"
	"github.com/Failover-bar/bot/wrap"
	"testing"
)

var pr *ProfileRepo

func TestProfile(t *testing.T) {
	pr = &ProfileRepo{DB: db}
	t.Run("create", testProfileCreateTable)
	t.Run("insert", testProfileInsert)
	t.Run("get", testProfileGet)
	t.Run("update", testProfileUpdate)
	t.Run("delete", testProfileDelete)
}

func testProfileCreateTable(t *testing.T) {
	if err := pr.CreateTable(context.Background()); err != nil {
		t.Error(err)
	}
}

func testProfileInsert(t *testing.T) {
	u := &Profile{
		UserID: userID,
	}
	err := pr.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}
}

func testProfileGet(t *testing.T) {
	u, err := pr.Get(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	if u.UserID != userID {
		t.Error("wrong user id", u)
	}
}

func testProfileUpdate(t *testing.T) {
	u, err := pr.Get(context.Background(), userID)
	if err != nil {
		t.Error("get: ", err)
	}
	u.Name = pointer.ToString("test")
	err = pr.Upsert(context.Background(), u)
	if err != nil {
		t.Error("upsert: ", err)
	}
	u, err = pr.Get(context.Background(), userID)
	if err != nil {
		t.Error("get: ", err)
	}
	if u.Name == nil || *u.Name != "test" {
		t.Error("nothing changed", u)
	}
}

func testProfileDelete(t *testing.T) {
	err := pr.Delete(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	_, err = pr.Get(context.Background(), userID)
	if !errors.Is(err, wrap.NotFoundError{}) {
		t.Error("not not_found error", err)
	}
}
