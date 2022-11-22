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
	IsRoleAdmin       bool   `db:"admin_option"`
}

func (c *Client) RoleHasMembers(ctx context.Context, roleID int64) (bool, error) {
	query := `SELECT EXISTS(SELECT "roleid" FROM "pg_catalog"."pg_auth_members" WHERE "roleid" = $1)`

	var ret bool
	err := c.db.QueryRow(ctx, query, roleID).Scan(&ret)
	if err != nil {
		return false, err
	}

	return ret, nil
}

func (c *Client) GetRoleByName(ctx context.Context, roleID string) (*RoleModel, error) {
	q := `
SELECT "rolname",
       "rolsuper",
       "rolinherit",
       "rolcreaterole",
       "rolcreatedb",
       "rolcanlogin",
       "rolreplication",
       "rolconnlimit",
       "rolbypassrls",
       "oid"::int
FROM "pg_catalog"."pg_roles"
WHERE "rolname" = $1
`

	role := &RoleModel{}
	err := pgxscan.Get(ctx, c.db, role, q, roleID)
	if err != nil {
		return nil, err
	}

	return role, nil
}

func (c *Client) GetRole(ctx context.Context, roleID int64) (*RoleModel, error) {
	q := `
SELECT "rolname",
       "rolsuper",
       "rolinherit",
       "rolcreaterole",
       "rolcreatedb",
       "rolcanlogin",
       "rolreplication",
       "rolconnlimit",
       "rolbypassrls",
       "oid"::int
FROM "pg_catalog"."pg_roles"
WHERE "oid" = $1
`

	role := &RoleModel{}
	err := pgxscan.Get(ctx, c.db, role, q, roleID)
	if err != nil {
		return nil, err
	}

	return role, nil
}

func (c *Client) ListRoleMembers(ctx context.Context, roleID int64, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing role members", zap.Int64("role_id", roleID))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT r."rolname",
       r."rolsuper",
       r."rolinherit",
       r."rolcreaterole",
       r."rolcreatedb",
       r."rolcanlogin",
       r."rolreplication",
       r."rolconnlimit",
       r."rolbypassrls",
       "oid"::int,
       m."admin_option"
FROM "pg_catalog"."pg_roles" r
         JOIN "pg_auth_members" m ON m."member" = r."oid"
WHERE m."roleid" = $1
ORDER BY r."rolname"
`)
	args = append(args, roleID)
	sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		sb.WriteString("OFFSET $3")
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

func (c *Client) ListRoles(ctx context.Context, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Info("listing roles")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	sb.WriteString(`
SELECT "rolname",
       "rolsuper",
       "rolinherit",
       "rolcreaterole",
       "rolcreatedb",
       "rolcanlogin",
       "rolreplication",
       "rolconnlimit",
       "rolbypassrls",
       "oid"::int
FROM "pg_catalog"."pg_roles"
ORDER BY "rolname"
`)
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
