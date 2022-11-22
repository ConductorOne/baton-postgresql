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

type RoleModel struct {
	ID                int64  `db:"oid"`
	Name              string `db:"rolname"`
	Superuser         bool   `db:"rolsuper"`
	Inherit           bool   `db:"rolinherit"`
	CreateRole        bool   `db:"rolcreaterole"`
	CreateDb          bool   `db:"rolcreatedb"`
	CanLogin          bool   `db:"rolcanlogin"`
	Replication       bool   `db:"rolreplication"`
	ConnectionLimit   int    `db:"rolconnlimit"`
	BypassRowSecurity bool   `db:"rolbypassrls"`
}

func (c *Client) ListRoles(ctx context.Context, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing roles")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`SELECT "rolname",
       						 "rolsuper",
       						 "rolinherit",
       						 "rolcreaterole",
       						 "rolcreatedb", 
       						 "rolcanlogin", 
       						 "rolreplication", 
       						 "rolconnlimit", 
       						 "rolbypassrls", 
       						 "oid"::int from "pg_catalog"."pg_roles" `)
	sb.WriteString("LIMIT $1 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $2")
		args = append(args, offset)
	}

	var ret []*RoleModel
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
