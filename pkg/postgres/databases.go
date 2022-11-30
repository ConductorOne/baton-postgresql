package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
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

func (c *Client) ListDatabases(ctx context.Context, pager *Pager) ([]*DatabaseModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing databases")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "oid"::int,
       "datname",
       "datdba",
       "datacl"
from "pg_catalog"."pg_database"
WHERE "datname"=$1
`)
	args = append(args, c.cfg.ConnConfig.Database)
	sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $3")
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
