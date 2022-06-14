package model

import (
	"context"
	"errors"
	"github.com/Failover-bar/bot/wrap"
	"testing"
	"time"
)

var ur *UserRepo

const userID = uint64(123)

func TestUser(t *testing.T) {
	ur = &UserRepo{DB: db}
	t.Run("create", testUserCreateTable)
	t.Run("insert", testUserInsert)
	t.Run("get", testUserGet)
	t.Run("update", testUserUpdate)
	t.Run("delete", testUserDelete)
}

func testUserCreateTable(t *testing.T) {
	if err := ur.CreateTable(context.Background()); err != nil {
		t.Error(err)
	}
}

func testUserInsert(t *testing.T) {
	u := &User{
		UserID: userID,
		State:  "active",
	}
	err := ur.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}
}

func testUserGet(t *testing.T) {
	u, err := ur.Get(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	if u.UserID != userID {
		t.Error("wrong user id", u)
	}

	if u.CreatedAt.IsZero() || u.LastAction.IsZero() {
		t.Error("onInsert failed", u)
	}
	if u.CreatedAt.After(time.Now()) {
		t.Error("user from future", u)
	}
}

func testUserUpdate(t *testing.T) {
	u, err := ur.Get(context.Background(), userID)
	if err != nil {
		t.Error("get:", err)
	}
	u.State = "updated"
	u.CreatedAt = u.CreatedAt.Add(-time.Minute)
	err = ur.Upsert(context.Background(), u)
	if err != nil {
		t.Error("upsert: ", err)
	}
	u, err = ur.Get(context.Background(), userID)
	if err != nil {
		t.Error("get: ", err)
	}
	if u.State != "updated" {
		t.Error("nothing changed", u)
	}
	if u.CreatedAt.Equal(u.LastAction) {
		t.Error("onUpdate failed", u)
	}
	if u.CreatedAt.After(u.LastAction) {
		t.Error("user from future", u)
	}
}

func testUserDelete(t *testing.T) {
	err := ur.Delete(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	_, err = ur.Get(context.Background(), userID)
	if !errors.Is(err, wrap.NotFoundError{}) {
		t.Error("not not_found error", err)
	}
}
