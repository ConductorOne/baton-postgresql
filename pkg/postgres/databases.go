package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	pgx "github.com/jackc/pgx/v4"
	"go.uber.org/zap"
)

type DatabaseModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"datname"`
	OwnerID int64    `db:"datdba"`
	ACLs    []string `db:"datacl"`
}

func (t *DatabaseModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *DatabaseModel) GetACLs() []string {
	return t.ACLs
}

func (t *DatabaseModel) AllPrivileges() PrivilegeSet {
	return Create | Temporary | Connect
}

func (t *DatabaseModel) DefaultPrivileges() PrivilegeSet {
	return Temporary | Connect
}

func (c *Client) GetDatabase(ctx context.Context, dbID int64) (*DatabaseModel, error) {
	ret := &DatabaseModel{}

	q := `
SELECT "oid"::int,
       "datname",
       "datdba",
       "datacl"
from "pg_catalog"."pg_database"
WHERE "oid"=$1
`

	err := pgxscan.Get(ctx, c.db, ret, q, dbID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) GetDatabaseByName(ctx context.Context, dbName string) (*DatabaseModel, error) {
	ret := &DatabaseModel{}

	q := `
SELECT "oid"::int,
       "datname",
       "datdba",
       "datacl"
from "pg_catalog"."pg_database"
WHERE "datname"=$1
`

	err := pgxscan.Get(ctx, c.db, ret, q, dbName)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) GetDatabaseById(ctx context.Context, dbId string) (*DatabaseModel, error) {
	ret := &DatabaseModel{}

	q := `
SELECT "oid"::int,
       "datname",
       "datdba",
       "datacl"
from "pg_catalog"."pg_database"
WHERE "oid"=$1
`

	err := pgxscan.Get(ctx, c.db, ret, q, dbId)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListDatabases(ctx context.Context, pager *Pager) ([]*DatabaseModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing databases")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT "oid"::int,
       "datname",
       "datdba",
       "datacl"
from "pg_catalog"."pg_database"
`)
	_, _ = sb.WriteString("LIMIT $1 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $2")
		args = append(args, offset)
	}

	var ret []*DatabaseModel
	err = pgxscan.Select(ctx, c.db, &ret, sb.String(), args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}

	var nextPageToken string
	if len(ret) > limit {
		offset += limit
		nextPageToken = strconv.Itoa(offset)
		ret = ret[:limit]
	}

	return ret, nextPageToken, nil
}

func (c *Client) CreateDatabase(ctx context.Context, dbName string) (*DatabaseModel, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("creating database", zap.String("dbName", dbName))

	sanitizedDbName := pgx.Identifier{dbName}.Sanitize()
	q := fmt.Sprintf("CREATE DATABASE %s", sanitizedDbName)
	_, err := c.db.Exec(ctx, q)
	if err != nil {
		return nil, err
	}
	return c.GetDatabaseByName(ctx, dbName)
}

func (c *Client) DeleteDatabase(ctx context.Context, dbName string) error {
	l := ctxzap.Extract(ctx)
	l.Debug("deleting database", zap.String("dbName", dbName))

	sanitizedDbName := pgx.Identifier{dbName}.Sanitize()
	q := fmt.Sprintf("DROP DATABASE %s", sanitizedDbName)
	_, err := c.db.Exec(ctx, q)
	return err
}

func transformPrivilege(privilege string) string {
	return strings.ReplaceAll(privilege, "-", "")
}

func sanitizePrivilege(privilege string) string {
	temp := pgx.Identifier{transformPrivilege(privilege)}.Sanitize()

	if strings.Count(privilege, "\"") != 2 {
		return strings.ReplaceAll(privilege, "\"", "")
	}

	return temp
}

func (c *Client) GrantDatabase(ctx context.Context, dbName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("granting database", zap.String("dbName", dbName), zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedDbName := pgx.Identifier{dbName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := pgx.Identifier{transformPrivilege(privilege)}.Sanitize()
	var q string
	if isGrant {
		q = fmt.Sprintf("GRANT %s ON DATABASE %s TO %s WITH GRANT OPTION", sanitizedPrivilege, sanitizedDbName, sanitizedPrincipalName)
	} else {
		q = fmt.Sprintf("GRANT %s ON DATABASE %s TO %s", sanitizedPrivilege, sanitizedDbName, sanitizedPrincipalName)
	}

	_, err := c.db.Exec(ctx, q)
	return err
}

func (c *Client) RevokeDatabase(ctx context.Context, dbName string, target string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)

	sanitizedDbName := pgx.Identifier{dbName}.Sanitize()
	sanitizedTarget := pgx.Identifier{target}.Sanitize()
	sanitizedPrivilege := pgx.Identifier{transformPrivilege(privilege)}.Sanitize()
	var q string
	if isGrant {
		q = fmt.Sprintf("REVOKE GRANT OPTION for %s ON DATABASE %s FROM %s", sanitizedPrivilege, sanitizedDbName, sanitizedTarget)
	} else {
		q = fmt.Sprintf("REVOKE %s ON DATABASE %s FROM %s", sanitizedPrivilege, sanitizedDbName, sanitizedTarget)
	}

	l.Debug("revoking role from member", zap.String("query", q))
	_, err := c.db.Exec(ctx, q)
	return err
}
