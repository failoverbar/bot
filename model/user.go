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

var writeTx = table.TxControl(
	table.BeginTx(
		table.WithSerializableReadWrite(),
	),
	table.CommitTx(),
)

type User struct {
	UserID uint64 `ydb:"user_id,primary"`
	Role   uint8  `ydb:"role"`

	State   string `ydb:"state"`
	Context string `ydb:"context"`

	CreatedAt  time.Time `ydb:"created_at"`
	LastAction time.Time `ydb:"last_action"`
	Version    uint32    `ydb:"version"`
}

func (u *User) BeforeInsert() {
	u.CreatedAt = time.Now()
	u.BeforeUpdate()
}

func (u *User) BeforeUpdate() {
	u.LastAction = time.Now()
}

func (u *User) scanValues() []named.Value {
	return []named.Value{
		named.Required("user_id", &u.UserID),
		named.OptionalWithDefault("role", &u.Role),
		named.OptionalWithDefault("state", &u.State),
		named.OptionalWithDefault("context", &u.Context),
		named.OptionalWithDefault("created_at", &u.CreatedAt),
		named.OptionalWithDefault("last_action", &u.LastAction),
		named.OptionalWithDefault("version", &u.Version),
	}
}

func (u *User) setValues() []table.ParameterOption {
	return []table.ParameterOption{
		table.ValueParam("$UserID", types.Uint64Value(u.UserID)),
		table.ValueParam("$Role", types.Uint8Value(u.Role)),
		table.ValueParam("$State", types.UTF8Value(u.State)),
		table.ValueParam("$Context", types.UTF8Value(u.Context)),
		table.ValueParam("$CreatedAt", types.DatetimeValueFromTime(u.CreatedAt)),
		table.ValueParam("$LastAction", types.DatetimeValueFromTime(u.LastAction)),
		table.ValueParam("$Version", types.Uint32Value(u.Version)),
	}
}

type UserRepo struct {
	DB ydb.Connection
}

func (ur UserRepo) declarePrimary() string {
	return `DECLARE $UserID AS Uint64;
`
}

func (ur UserRepo) declareUser() string {
	return `
		DECLARE $UserID AS Uint64;
		DECLARE $Role AS Uint8;
		DECLARE $State AS Utf8;
		DECLARE $Context AS Utf8;
		DECLARE $CreatedAt AS Datetime;
		DECLARE $LastAction AS Datetime;
		DECLARE $Version AS Uint32;
`
}

func (ur UserRepo) fields() string {
	return ` user_id, role, state, context, created_at, last_action, version `
}

func (ur UserRepo) values() string {
	return ` ($UserID, $Role, $State, $Context, $CreatedAt, $LastAction, $Version) `
}

func (ur UserRepo) table(name string) string {
	res := ` users `
	if name != "" {
		res += name + ` `
	}
	return res
}

func (ur UserRepo) findPrimary() string {
	return ` WHERE user_id = $UserID `
}

func (ur UserRepo) primaryParams(userID uint64) *table.QueryParameters {
	return table.NewQueryParameters(table.ValueParam("$UserID", types.Uint64Value(userID)))
}

func (ur *UserRepo) Get(ctx context.Context, userID uint64) (u *User, err error) {
	defer wrap.Errf("get user %d", &err, userID)
	u = &User{}
	query := ur.declarePrimary() + `SELECT ` + ur.fields() +
		" FROM " + ur.table("") +
		ur.findPrimary()
	var res result.Result
	err = ur.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, res, err = s.Execute(ctx, table.DefaultTxControl(), query,
			ur.primaryParams(userID),
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

func (ur *UserRepo) Insert(ctx context.Context, u *User) (err error) {
	defer wrap.Errf("insert user %d", &err, u.UserID)
	u.BeforeInsert()
	query := ur.declareUser() + `INSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *UserRepo) Upsert(ctx context.Context, u *User) (err error) {
	defer wrap.Errf("upsert user %d", &err, u.UserID)
	u.BeforeUpdate()
	query := ur.declareUser() + `UPSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *UserRepo) Delete(ctx context.Context, userID uint64) (err error) {
	defer wrap.Errf("delete user %d", &err, userID)
	query := ur.declarePrimary() + `DELETE FROM ` + ur.table("") + ur.findPrimary()
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				ur.primaryParams(userID),
				options.WithCollectStatsModeBasic(),
			)
			return err
		},
	)
}

func (ur *UserRepo) CreateTable(ctx context.Context) (err error) {
	defer wrap.Err("create table", &err)
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ur.DB.Name(), "users"),
				options.WithColumn("user_id", types.Optional(types.TypeUint64)),
				options.WithColumn("role", types.Optional(types.TypeUint8)),
				options.WithColumn("state", types.Optional(types.TypeUTF8)),
				options.WithColumn("context", types.Optional(types.TypeUTF8)),
				options.WithColumn("created_at", types.Optional(types.TypeDatetime)),
				options.WithColumn("last_action", types.Optional(types.TypeDatetime)),
				options.WithColumn("version", types.Optional(types.TypeUint32)),
				options.WithPrimaryKeyColumn("user_id"),
			)
		},
	)
}
