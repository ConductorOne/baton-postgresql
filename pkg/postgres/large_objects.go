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

type LargeObjectModel struct {
	ID      int64    `db:"oid"`
	OwnerID int64    `db:"lomowner"`
	ACLs    []string `db:"lomacl"`
}

func (t *LargeObjectModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *LargeObjectModel) GetACLs() []string {
	return t.ACLs
}

func (t *LargeObjectModel) AllPrivileges() PrivilegeSet {
	return Select | Update
}

func (t *LargeObjectModel) DefaultPrivileges() PrivilegeSet {
	return EmptyPrivilegeSet
}

func (c *Client) ListLargeObjects(ctx context.Context, pager *Pager) ([]*LargeObjectModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing large objects")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "oid"::int,
       "lomowner",
       "lomacl"
from "pg_catalog"."pg_largeobject_metadata"
`)
	sb.WriteString("LIMIT $1 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $2")
		args = append(args, offset)
	}

	var ret []*LargeObjectModel
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

func (c *Client) GetLargeObject(ctx context.Context, largeObjectID int64) (*LargeObjectModel, error) {
	l := ctxzap.Extract(ctx)
	l.Info("getting large object")

	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "oid"::int,
       "lomowner",
       "lomacl"
from "pg_catalog"."pg_largeobject_metadata"
WHERE oid=$1
`)

	var ret LargeObjectModel
	err := pgxscan.Get(ctx, c.db, &ret, sb.String(), largeObjectID)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}
