package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type ProcedureModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"proname"`
	Schema  string   `db:"nspname"`
	OwnerID int64    `db:"proowner"`
	ACLs    []string `db:"proacl"`
}

func (c *Client) GetProcedure(ctx context.Context, functionID int64) (*ProcedureModel, error) {
	ret := &ProcedureModel{}

	q := `
SELECT a."oid"::int, a."proname",
       n."nspname",
       a."proowner"::int, a."proacl"
FROM "pg_catalog"."pg_proc" a
         LEFT JOIN pg_namespace n ON n."oid" = a."pronamespace"
WHERE a."oid" = $1
`

	err := pgxscan.Get(ctx, c.db, ret, q, functionID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListProcedures(ctx context.Context, schemaID int64, pager *Pager) ([]*ProcedureModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing procedures for schema", zap.Int64("schema_id", schemaID))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
select a."oid"::int, a."proname", n."nspname", a."proowner"::int, a."proacl"
from "pg_catalog"."pg_proc" a
         LEFT JOIN pg_namespace n ON n."oid" = a."pronamespace"
where a."prokind" = 'p'
  and a."pronamespace" = $1
`)
	args = append(args, schemaID)
	sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*ProcedureModel
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
