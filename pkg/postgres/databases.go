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

func (c *Client) ListDatabases(ctx context.Context, pager *Pager) ([]*DatabaseModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing databases")

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
