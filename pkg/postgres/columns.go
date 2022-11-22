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

type ColumnModel struct {
	ID        int64    `db:"attnum"`
	Name      string   `db:"attname"`
	TableName string   `db:"tablename"`
	ACLs      []string `db:"attacl"`
}

func (c *Client) ListColumns(ctx context.Context, tableID int64, pager *Pager) ([]*ColumnModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing columns for table", zap.Int64("table_id", tableID))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "attnum",
       "attname",
       "attacl"
FROM "pg_catalog"."pg_attribute"
WHERE "attrelid" = $1
  AND "attnum" > 0
  AND NOT "attisdropped"
`)

	args = append(args, tableID)
	sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*ColumnModel
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
