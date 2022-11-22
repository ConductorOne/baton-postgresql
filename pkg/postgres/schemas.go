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

type SchemaModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"nspname"`
	OwnerID int64    `db:"nspowner"`
	ACLs    []string `db:"nspacl"`
}

func (c *Client) ListSchemas(ctx context.Context, pager *Pager) ([]*SchemaModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing roles")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`SELECT 	"oid"::int,
								"nspname",  
								"nspowner",
								"nspacl"
								from "pg_catalog"."pg_namespace" `)
	sb.WriteString("LIMIT $1 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $2")
		args = append(args, offset)
	}

	var ret []*SchemaModel
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
