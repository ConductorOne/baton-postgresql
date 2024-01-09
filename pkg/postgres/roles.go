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
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
)

type RoleModel struct {
	ID                int64   `db:"oid"`
	Name              string  `db:"rolname"`
	Superuser         bool    `db:"rolsuper"`
	Inherit           bool    `db:"rolinherit"`
	CreateRole        bool    `db:"rolcreaterole"`
	CreateDb          bool    `db:"rolcreatedb"`
	CanLogin          bool    `db:"rolcanlogin"`
	Replication       bool    `db:"rolreplication"`
	ConnectionLimit   int     `db:"rolconnlimit"`
	BypassRowSecurity bool    `db:"rolbypassrls"`
	RoleAdmin         *bool   `db:"admin_option"`
	MemberOf          []int64 `db:"member_of"`
}

func (r *RoleModel) IsRoleAdmin() bool {
	if r.RoleAdmin == nil {
		return false
	}

	return *r.RoleAdmin
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

func (c *Client) GetRoleByName(ctx context.Context, roleName string) (*RoleModel, error) {
	q := `
SELECT r."rolname",
       r."rolsuper",
       r."rolinherit",
       r."rolcreaterole",
       r."rolcreatedb",
       r."rolcanlogin",
       r."rolreplication",
       r."rolconnlimit",
       r."rolbypassrls",
       r."oid"::int,
       m."admin_option",
       ARRAY
           (SELECT "roleid"::int
            FROM "pg_catalog"."pg_auth_members"
            where member = r."oid")
           AS "member_of"
FROM "pg_catalog"."pg_roles" r
         LEFT JOIN "pg_auth_members" m ON m."member" = r."oid"
WHERE r."rolname" = $1
`

	role := &RoleModel{}
	err := pgxscan.Get(ctx, c.db, role, q, roleName)
	if err != nil {
		return nil, err
	}

	return role, nil
}

func (c *Client) GetRole(ctx context.Context, roleID int64) (*RoleModel, error) {
	q := `
SELECT DISTINCT
       r."rolname",
       r."rolsuper",
       r."rolinherit",
       r."rolcreaterole",
       r."rolcreatedb",
       r."rolcanlogin",
       r."rolreplication",
       r."rolconnlimit",
       r."rolbypassrls",
       r."oid"::int,
       m."admin_option",
       ARRAY(SELECT "roleid"::int
             FROM "pg_catalog"."pg_auth_members"
             where member = r."oid") AS "member_of"
FROM "pg_catalog"."pg_roles" r
         LEFT JOIN "pg_auth_members" m ON m."member" = r."oid"
WHERE r."oid" = $1
`

	role := &RoleModel{}
	err := pgxscan.Get(ctx, c.db, role, q, roleID)
	if err != nil {
		return nil, err
	}

	return role, nil
}

func (c *Client) GrantRole(ctx context.Context, roleName string, principalName string) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()

	query := "GRANT " + sanitizedRoleName + " TO " + sanitizedPrincipalName
	l.Debug("granting role to member", zap.String("query", query))

	_, err := c.db.Exec(ctx, query)
	return err
}

func (c *Client) RevokeRole(ctx context.Context, roleName string, target string, isGrant bool) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	sanitizedTarget := pgx.Identifier{target}.Sanitize()

	query := "REVOKE " + sanitizedRoleName + " FROM " + sanitizedTarget

	if isGrant {
		query = "REVOKE GRANT OPTION FOR " + sanitizedRoleName + " FROM " + sanitizedTarget
	}

	l.Debug("revoking role from member", zap.String("query", query))
	_, err := c.db.Exec(ctx, query)
	return err
}

func (c *Client) CreateRole(ctx context.Context, roleName string) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	query := "CREATE ROLE " + sanitizedRoleName

	l.Debug("creating role", zap.String("query", query))
	_, err := c.db.Exec(ctx, query)
	return err
}

func (c *Client) DeleteRole(ctx context.Context, roleName string) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	query := "DROP ROLE " + sanitizedRoleName

	l.Debug("deleting role", zap.String("query", query))
	_, err := c.db.Exec(ctx, query)
	return err
}

func (c *Client) CreateUser(ctx context.Context, login string, password string) (*RoleModel, error) {
	l := ctxzap.Extract(ctx)

	sanitizedLogin := pgx.Identifier{login}.Sanitize()
	query := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD $1", sanitizedLogin)

	l.Debug("creating user", zap.String("query", query))

	_, err := c.db.Exec(ctx, query, pgx.QuerySimpleProtocol(true), password)
	if err != nil {
		return nil, err
	}
	return c.GetRoleByName(ctx, login)
}

func (c *Client) ChangePassword(ctx context.Context, userName string, password string) (*RoleModel, error) {
	l := ctxzap.Extract(ctx)

	sanitizedUserName := pgx.Identifier{userName}.Sanitize()

	query := fmt.Sprintf("ALTER USER %s WITH PASSWORD $1", sanitizedUserName)

	l.Debug("changing password for user", zap.String("query", query))

	_, err := c.db.Exec(ctx, query, pgx.QuerySimpleProtocol(true), password)
	if err != nil {
		return nil, err
	}
	return c.GetRoleByName(ctx, userName)
}

func (c *Client) ListRoleMembers(ctx context.Context, roleID int64, pager *Pager) ([]*RoleModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing role members", zap.Int64("role_id", roleID))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT r."rolname",
       r."rolsuper",
       r."rolinherit",
       r."rolcreaterole",
       r."rolcreatedb",
       r."rolcanlogin",
       r."rolreplication",
       r."rolconnlimit",
       r."rolbypassrls",
       r."oid"::int,
       m."admin_option",
       ARRAY(SELECT "roleid"::int
             FROM "pg_catalog"."pg_auth_members"
             where member = r."oid") AS "member_of"
FROM "pg_catalog"."pg_roles" r
         LEFT JOIN "pg_auth_members" m ON m."member" = r."oid"
WHERE m."roleid" = $1
ORDER BY r."rolname"
`)
	args = append(args, roleID)
	_, _ = sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $3")
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
	l.Debug("listing roles")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT r."rolname",
       r."rolsuper",
       r."rolinherit",
       r."rolcreaterole",
       r."rolcreatedb",
       r."rolcanlogin",
       r."rolreplication",
       r."rolconnlimit",
       r."rolbypassrls",
       r."oid"::int,
       m."admin_option",
       ARRAY(SELECT "roleid"::int
             FROM "pg_catalog"."pg_auth_members"
             where member = r."oid")
           AS "member_of"
FROM "pg_catalog"."pg_roles" r
         LEFT JOIN "pg_auth_members" m ON m."member" = r."oid"
ORDER BY "rolname"
`)
	_, _ = sb.WriteString("LIMIT $1 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $2")
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
