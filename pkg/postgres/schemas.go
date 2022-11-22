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
)

type SchemaModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"nspname"`
	OwnerID int64    `db:"nspowner"`
	ACLs    []string `db:"nspacl"`
}

func (t *SchemaModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *SchemaModel) GetACLs() []string {
	return t.ACLs
}

func (t *SchemaModel) AllPrivileges() PrivilegeSet {
	return Usage | Create
}

func (t *SchemaModel) DefaultPrivileges() PrivilegeSet {
	return EmptyPrivilegeSet
}

func (c *Client) GetSchema(ctx context.Context, schemaID int64) (*SchemaModel, error) {
	ret := &SchemaModel{}

	q := `
SELECT "oid"::int, "nspname",
       "nspowner",
       "nspacl"
FROM "pg_catalog"."pg_namespace"
WHERE "oid" = $1
`

	err := pgxscan.Get(ctx, c.db, ret, q, schemaID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListSchemas(ctx context.Context, pager *Pager) ([]*SchemaModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing schemas")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "oid"::int, "nspname",
       "nspowner",
       "nspacl"
from "pg_catalog"."pg_namespace"
`)
	if len(c.schemaFilter) > 0 {
		sb.WriteString("WHERE ")
		for ii, s := range c.schemaFilter {
			if ii != 0 {
				sb.WriteString("OR ")
			}
			sb.WriteString(fmt.Sprintf(`"nspname" = $%d `, len(args)+1))
			args = append(args, s)
		}
	}

	sb.WriteString(fmt.Sprintf("LIMIT $%d ", len(args)+1))
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString(fmt.Sprintf("OFFSET $%d ", len(args)+1))
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
