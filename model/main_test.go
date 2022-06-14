package model

import (
	"context"
	ydbEnviron "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"log"
	"os"
	"testing"
)

var db ydb.Connection

func TestMain(m *testing.M) {
	ctx := context.Background()
	dsn, ok := os.LookupEnv("YDB_DSN")
	if !ok {
		log.Fatal("Set env YDB_DSN and any cred from ydb-go-sdk-auth-environ")
	}
	var err error
	db, err = ydb.Open(ctx, dsn, ydbEnviron.WithEnvironCredentials(ctx))
	if err != nil {
		log.Fatal("can't connect to DB", err)
	}

	os.Exit(m.Run())
}
