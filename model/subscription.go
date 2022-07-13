package model

import (
	"context"
	"github.com/failoverbar/bot/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"path"
	"time"
)

type Subscription struct {
	UserID uint64 `ydb:"user_id,primary"`
	Topic  string `ydb:"topic,primary"`

	Active bool `ydb:"active"`

	CreatedAt  time.Time `ydb:"created_at"`
	LastAction time.Time `ydb:"last_action"`
}

func (u *Subscription) BeforeInsert() {
	u.CreatedAt = time.Now()
	u.BeforeUpdate()
}

func (u *Subscription) BeforeUpdate() {
	u.LastAction = time.Now()
}

func (u *Subscription) scanValues() []named.Value {
	return []named.Value{
		named.Required("user_id", &u.UserID),
		named.OptionalWithDefault("topic", &u.Topic),
		named.OptionalWithDefault("active", &u.Active),
		named.OptionalWithDefault("created_at", &u.CreatedAt),
		named.OptionalWithDefault("last_action", &u.LastAction),
	}
}

func (u *Subscription) setValues() []table.ParameterOption {
	return []table.ParameterOption{
		table.ValueParam("$UserID", types.Uint64Value(u.UserID)),
		table.ValueParam("$Topic", types.UTF8Value(u.Topic)),
		table.ValueParam("$Active", types.BoolValue(u.Active)),
		table.ValueParam("$CreatedAt", types.DatetimeValueFromTime(u.CreatedAt)),
		table.ValueParam("$LastAction", types.DatetimeValueFromTime(u.LastAction)),
	}
}

type SubscriptionRepo struct {
	DB ydb.Connection
}

func (ur SubscriptionRepo) declarePrimary() string {
	return `
		DECLARE $UserID AS Uint64;
		DECLARE $Topic AS Utf8;
`
}

func (ur SubscriptionRepo) declareSubscription() string {
	return `
		DECLARE $UserID AS Uint64;
		DECLARE $Topic AS Utf8;
		DECLARE $Active AS Bool;
		DECLARE $CreatedAt AS Datetime;
		DECLARE $LastAction AS Datetime;
`
}

func (ur SubscriptionRepo) fields() string {
	return ` user_id, topic, active, created_at, last_action `
}

func (ur SubscriptionRepo) values() string {
	return ` ($UserID, $Topic, $Active, $CreatedAt, $LastAction) `
}

func (ur SubscriptionRepo) table(name string) string {
	res := ` subscriptions `
	if name != "" {
		res += name + ` `
	}
	return res
}

func (ur SubscriptionRepo) findPrimary() string {
	return ` WHERE user_id = $UserID AND topic = $Topic `
}

func (ur SubscriptionRepo) findByFirst() string {
	return ` WHERE user_id = $UserID `
}

func (ur SubscriptionRepo) firstParam(userID uint64) *table.QueryParameters {
	return table.NewQueryParameters(
		table.ValueParam("$UserID", types.Uint64Value(userID)),
	)
}

func (ur SubscriptionRepo) primaryParams(userID uint64, topic string) *table.QueryParameters {
	return table.NewQueryParameters(
		table.ValueParam("$UserID", types.Uint64Value(userID)),
		table.ValueParam("$Topic", types.UTF8Value(topic)),
	)
}

func (ur *SubscriptionRepo) Get(ctx context.Context, userID uint64, topic string) (u *Subscription, err error) {
	defer wrap.Errf("get subscription %d,%s", &err, userID, topic)
	u = &Subscription{}
	query := ur.declarePrimary() + `SELECT ` + ur.fields() +
		" FROM " + ur.table("") +
		ur.findPrimary()
	var res result.Result
	err = ur.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, res, err = s.Execute(ctx, table.DefaultTxControl(), query,
			ur.primaryParams(userID, topic),
			options.WithCollectStatsModeBasic(),
		)
		return err
	})
	if err != nil {
		return
	}
	defer func() {
		_ = res.Close()
	}()
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanNamed(u.scanValues()...)
			return
		}
	}
	err = wrap.NotFoundError{}
	return
}

func (ur *SubscriptionRepo) GetByUserID(ctx context.Context, userID uint64) (ss []*Subscription, err error) {
	defer wrap.Errf("get subscriptions by userID %d", &err, userID)
	query := ur.declarePrimary() + `SELECT ` + ur.fields() +
		" FROM " + ur.table("") +
		ur.findByFirst()
	var res result.Result
	err = ur.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, res, err = s.Execute(ctx, table.DefaultTxControl(), query,
			ur.firstParam(userID),
			options.WithCollectStatsModeBasic(),
		)
		return err
	})
	if err != nil {
		return
	}
	defer func() {
		_ = res.Close()
	}()
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			s := &Subscription{}
			err = res.ScanNamed(s.scanValues()...)
			if err != nil {
				return
			}
			ss = append(ss, s)
		}
	}
	return
}

func (ur *SubscriptionRepo) Insert(ctx context.Context, u *Subscription) (err error) {
	defer wrap.Errf("insert subscription %d,%s", &err, u.UserID, u.Topic)
	u.BeforeInsert()
	query := ur.declareSubscription() + `INSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(u.setValues()...),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
}

func (ur *SubscriptionRepo) Upsert(ctx context.Context, u *Subscription) (err error) {
	defer wrap.Errf("upsert subscription %d,%s", &err, u.UserID, u.Topic)
	u.BeforeUpdate()
	query := ur.declareSubscription() + `UPSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(u.setValues()...),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
}

func (ur *SubscriptionRepo) Delete(ctx context.Context, userID uint64, topic string) (err error) {
	defer wrap.Errf("delete subscription %d,%s", &err, userID, topic)
	query := ur.declarePrimary() + `DELETE FROM ` + ur.table("") + ur.findPrimary()
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				ur.primaryParams(userID, topic),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
}

func (ur *SubscriptionRepo) DeleteByUserID(ctx context.Context, userID uint64) (err error) {
	defer wrap.Errf("delete subscription by userID %d", &err, userID)
	query := ur.declarePrimary() + `DELETE FROM ` + ur.table("") + ur.findByFirst()
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				ur.firstParam(userID),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
}

func (ur *SubscriptionRepo) CreateTable(ctx context.Context) (err error) {
	defer wrap.Err("create table", &err)
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ur.DB.Name(), "subscriptions"),
				options.WithColumn("user_id", types.Optional(types.TypeUint64)),
				options.WithColumn("topic", types.Optional(types.TypeUTF8)),
				options.WithColumn("active", types.Optional(types.TypeBool)),
				options.WithColumn("created_at", types.Optional(types.TypeDatetime)),
				options.WithColumn("last_action", types.Optional(types.TypeDatetime)),
				options.WithPrimaryKeyColumn("user_id", "topic"),
			)
		},
	)
}
