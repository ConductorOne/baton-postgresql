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

type SequenceModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"relname"`
	Schema  string   `db:"nspname"`
	OwnerID int64    `db:"relowner"`
	ACLs    []string `db:"relacl"`
}

func (c *Client) GetSequence(ctx context.Context, sequenceID int64) (*SequenceModel, error) {
	ret := &SequenceModel{}

	q := `
SELECT c."oid"::int, c."relname", c."relowner"::int, n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE c."oid" = $1
`

	err := pgxscan.Get(ctx, c.db, ret, q, sequenceID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListSequences(ctx context.Context, schemaID int64, pager *Pager) ([]*SequenceModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing sequences")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT c."oid"::int, c."relname", c."relowner"::int, n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE n."oid" = $1
  AND (c."relkind" = 'S')
`)

	args = append(args, schemaID)
	sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*SequenceModel
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
