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

type Profile struct {
	UserID uint64 `ydb:"user_id,primary"`

	Name   *string `ydb:"name"`
	Phone  *string `ydb:"phone"`
	Email  *string `ydb:"email"`
	Source string  `ydb:"source"`
}

func (u *Profile) scanValues() []named.Value {
	return []named.Value{
		named.Required("user_id", &u.UserID),
		named.Optional("name", &u.Name),
		named.Optional("phone", &u.Phone),
		named.Optional("email", &u.Email),
		named.OptionalWithDefault("source", &u.Source),
	}
}

func (u *Profile) setValues() []table.ParameterOption {
	return []table.ParameterOption{
		table.ValueParam("$UserID", types.Uint64Value(u.UserID)),
		table.ValueParam("$Name", types.NullableUTF8Value(u.Name)),
		table.ValueParam("$Phone", types.NullableUTF8Value(u.Phone)),
		table.ValueParam("$Email", types.NullableUTF8Value(u.Email)),
		table.ValueParam("$Source", types.UTF8Value(u.Source)),
	}
}

type ProfileRepo struct {
	DB ydb.Connection
}

func (ur ProfileRepo) declarePrimary() string {
	return `DECLARE $UserID AS Uint64;
`
}

func (ur ProfileRepo) declareProfile() string {
	return `
		DECLARE $UserID AS Uint64;
		DECLARE $Name AS Utf8?;
		DECLARE $Phone AS Utf8?;
		DECLARE $Email AS Utf8?;
		DECLARE $Source AS Utf8;
`
}

func (ur ProfileRepo) fields() string {
	return ` user_id, name, phone, email, source `
}

func (ur ProfileRepo) values() string {
	return ` ($UserID, $Name, $Phone, $Email, $Source) `
}

func (ur ProfileRepo) table(name string) string {
	res := ` profiles `
	if name != "" {
		res += name + ` `
	}
	return res
}

func (ur ProfileRepo) findPrimary() string {
	return ` WHERE user_id = $UserID `
}

func (ur ProfileRepo) primaryParams(profileID uint64) *table.QueryParameters {
	return table.NewQueryParameters(table.ValueParam("$UserID", types.Uint64Value(profileID)))
}

func (ur *ProfileRepo) Get(ctx context.Context, userID uint64) (u *Profile, err error) {
	defer wrap.Errf("get profile %d", &err, userID)
	u = &Profile{}
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

func (ur *ProfileRepo) Insert(ctx context.Context, u *Profile) (err error) {
	defer wrap.Errf("insert profile %d", &err, u.UserID)
	query := ur.declareProfile() + `INSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *ProfileRepo) Upsert(ctx context.Context, u *Profile) (err error) {
	defer wrap.Errf("upsert profile %d", &err, u.UserID)
	query := ur.declareProfile() + `UPSERT INTO ` + ur.table("") + ` (` + ur.fields() + `) VALUES ` + ur.values()
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

func (ur *ProfileRepo) Delete(ctx context.Context, userID uint64) (err error) {
	defer wrap.Errf("delete profile %d", &err, userID)
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

func (ur *ProfileRepo) CreateTable(ctx context.Context) (err error) {
	defer wrap.Err("create table", &err)
	return ur.DB.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(ur.DB.Name(), "profiles"),
				options.WithColumn("user_id", types.Optional(types.TypeUint64)),
				options.WithColumn("name", types.Optional(types.TypeUTF8)),
				options.WithColumn("phone", types.Optional(types.TypeUTF8)),
				options.WithColumn("email", types.Optional(types.TypeUTF8)),
				options.WithColumn("source", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("user_id"),
			)
		},
	)
}
