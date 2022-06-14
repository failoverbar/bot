package model

import (
	"context"
	"github.com/Failover-bar/bot/wrap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"path"
)

//user_id Uint64,

//username Utf8,
//first_name Utf8,
//last_name Utf8,
//language_code Utf8,

type TelegramProfile struct {
	UserID uint64 `ydb:"user_id,primary"`

	Username     string `ydb:"username"`
	FirstName    string `ydb:"first_name"`
	LastName     string `ydb:"last_name"`
	LanguageCode string `ydb:"language_code"`
}

func (u *TelegramProfile) scanValues() []named.Value {
	return []named.Value{
		named.Required("user_id", &u.UserID),
		named.OptionalWithDefault("username", &u.Username),
		named.OptionalWithDefault("first_name", &u.FirstName),
		named.OptionalWithDefault("last_name", &u.LastName),
		named.OptionalWithDefault("language_code", &u.LanguageCode),
	}
}

func (u *TelegramProfile) setValues() []table.ParameterOption {
	return []table.ParameterOption{
		table.ValueParam("$UserID", types.Uint64Value(u.UserID)),
		table.ValueParam("$Username", types.UTF8Value(u.Username)),
		table.ValueParam("$FirstName", types.UTF8Value(u.FirstName)),
		table.ValueParam("$LastName", types.UTF8Value(u.LastName)),
		table.ValueParam("$LanguageCode", types.UTF8Value(u.LanguageCode)),
	}
}

type TelegramProfileRepo struct {
	DB ydb.Connection
}

func (ur TelegramProfileRepo) declarePrimary() string {
	return `DECLARE $UserID AS Uint64;
`
}

func (ur TelegramProfileRepo) declareTelegramProfile() string {
	return `
		DECLARE $UserID AS Uint64;
		DECLARE $Username AS Utf8;
		DECLARE $FirstName AS Utf8;
		DECLARE $LastName AS Utf8;
		DECLARE $LanguageCode AS Utf8;
`
}

func (ur TelegramProfileRepo) fields() string {
	return ` user_id, username, first_name, last_name, language_code `
}

func (ur TelegramProfileRepo) values() string {
	return ` ($UserID, $Username, $FirstName, $LastName, $LanguageCode) `
}

func (ur TelegramProfileRepo) table(name string) string {
	res := ` telegram_profiles `
	if name != "" {
		res += name + ` `
	}
	return res
}

func (ur TelegramProfileRepo) findPrimary() string {
	return ` WHERE user_id = $UserID `
}

func (ur TelegramProfileRepo) primaryParams(telegramProfileID uint64) *table.QueryParameters {
	return table.NewQueryParameters(table.ValueParam("$UserID", types.Uint64Value(telegramProfileID)))
}

func (ur *TelegramProfileRepo) Get(ctx context.Context, userID uint64) (u *TelegramProfile, err error) {
	defer wrap.Errf("get telegram profile %d", &err, userID)
	u = &TelegramProfile{}
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

func (ur *TelegramProfileRepo) Insert(ctx context.Context, u *TelegramProfile) (err error) {
	defer wrap.Errf("insert telegram profile %d", &err, u.UserID)
	query := ur.declareTelegramProfile() + `INSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *TelegramProfileRepo) Upsert(ctx context.Context, u *TelegramProfile) (err error) {
	defer wrap.Errf("upsert telegram profile %d", &err, u.UserID)
	query := ur.declareTelegramProfile() + `UPSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *TelegramProfileRepo) Delete(ctx context.Context, userID uint64) (err error) {
	defer wrap.Errf("delete telegram profile %d", &err, userID)
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

func (ur *TelegramProfileRepo) CreateTable(ctx context.Context) (err error) {
	defer wrap.Err("create table", &err)
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ur.DB.Name(), "telegram_profiles"),
				options.WithColumn("user_id", types.Optional(types.TypeUint64)),
				options.WithColumn("username", types.Optional(types.TypeUTF8)),
				options.WithColumn("first_name", types.Optional(types.TypeUTF8)),
				options.WithColumn("last_name", types.Optional(types.TypeUTF8)),
				options.WithColumn("language_code", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("user_id"),
			)
		},
	)
}
