package model

import (
	"context"
	"errors"
	"github.com/Failover-bar/bot/wrap"
	"testing"
	"time"
)

var sr *SubscriptionRepo

const topic = "topic"
const topic2 = "topic2"
const topic3 = "topic3"

func TestSubscription(t *testing.T) {
	sr = &SubscriptionRepo{DB: db}
	t.Run("create", testSubscriptionCreateTable)
	t.Run("insert", testSubscriptionInsert)
	t.Run("get", testSubscriptionGet)
	t.Run("getByUserID", testSubscriptionGetByUserID)
	t.Run("update", testSubscriptionUpdate)
	t.Run("delete", testSubscriptionDelete)
	t.Run("deleteByUserID", testSubscriptionDeleteByUserID)
}

func testSubscriptionCreateTable(t *testing.T) {
	if err := sr.CreateTable(context.Background()); err != nil {
		t.Error(err)
	}
}

func testSubscriptionInsert(t *testing.T) {
	u := &Subscription{
		UserID: userID,
		Topic:  topic,
		Active: true,
	}
	err := sr.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}
}

func testSubscriptionGet(t *testing.T) {
	u, err := sr.Get(context.Background(), userID, topic)
	if err != nil {
		t.Error(err)
	}
	if u.UserID != userID {
		t.Error("wrong subscription id", u)
	}

	if u.CreatedAt.IsZero() || u.LastAction.IsZero() {
		t.Error("onInsert failed", u)
	}
	if u.CreatedAt.After(time.Now()) {
		t.Error("subscription from future", u)
	}
}

func testSubscriptionGetByUserID(t *testing.T) {
	u := &Subscription{
		UserID: userID,
		Topic:  topic2,
		Active: true,
	}
	err := sr.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}

	u = &Subscription{
		UserID: userID,
		Topic:  topic3,
		Active: true,
	}
	err = sr.Insert(context.Background(), u)
	if err != nil {
		t.Error(err)
	}

	ss, err := sr.GetByUserID(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	if len(ss) != 3 {
		t.Error("wrong users topics count", len(ss))
	}
}

func testSubscriptionUpdate(t *testing.T) {
	u, err := sr.Get(context.Background(), userID, topic)
	if err != nil {
		t.Error("get:", err)
	}
	u.Active = false
	u.CreatedAt = u.CreatedAt.Add(-time.Minute)
	err = sr.Upsert(context.Background(), u)
	if err != nil {
		t.Error("upsert: ", err)
	}
	u, err = sr.Get(context.Background(), userID, topic)
	if err != nil {
		t.Error("get: ", err)
	}
	if u.Active {
		t.Error("nothing changed", u)
	}
	if u.CreatedAt.Equal(u.LastAction) {
		t.Error("onUpdate failed", u)
	}
	if u.CreatedAt.After(u.LastAction) {
		t.Error("subscription from future", u)
	}
}

func testSubscriptionDelete(t *testing.T) {
	err := sr.Delete(context.Background(), userID, topic)
	if err != nil {
		t.Error(err)
	}
	_, err = sr.Get(context.Background(), userID, topic)
	if !errors.Is(err, wrap.NotFoundError{}) {
		t.Error("not not_found error", err)
	}
	_, err = sr.Get(context.Background(), userID, topic2)
	if err != nil {
		t.Error("topic2 must exists", err)
	}
}

func testSubscriptionDeleteByUserID(t *testing.T) {
	err := sr.DeleteByUserID(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	ss, err := sr.GetByUserID(context.Background(), userID)
	if err != nil {
		t.Error(err)
	}
	if len(ss) != 0 {
		t.Error("user topics must by erased", len(ss))
	}
}
