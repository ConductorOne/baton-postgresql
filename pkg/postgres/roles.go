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
	pgx "github.com/jackc/pgx/v4"
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

	if roleName == "" {
		return errors.New("role name cannot be empty")
	}

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	query := "CREATE ROLE " + sanitizedRoleName

	l.Debug("creating role", zap.String("query", query))
	_, err := c.db.Exec(ctx, query)
	return err
}

// RoleOwnsObjects checks if a role owns any database objects.
func (c *Client) RoleOwnsObjects(ctx context.Context, roleName string) (bool, error) {
	l := ctxzap.Extract(ctx)

	query := `
		SELECT EXISTS(
			SELECT 1 FROM (
				-- Check for owned schemas
				SELECT 1 FROM pg_namespace WHERE nspowner = (SELECT oid FROM pg_roles WHERE rolname = $1)
				UNION ALL
				-- Check for owned tables
				SELECT 1 FROM pg_class WHERE relowner = (SELECT oid FROM pg_roles WHERE rolname = $1)
				UNION ALL
				-- Check for owned functions
				SELECT 1 FROM pg_proc WHERE proowner = (SELECT oid FROM pg_roles WHERE rolname = $1)
				UNION ALL
				-- Check for owned sequences
				SELECT 1 FROM pg_class WHERE relowner = (SELECT oid FROM pg_roles WHERE rolname = $1) AND relkind = 'S'
				UNION ALL
				-- Check for owned views
				SELECT 1 FROM pg_class WHERE relowner = (SELECT oid FROM pg_roles WHERE rolname = $1) AND relkind = 'v'
				UNION ALL
				-- Check for owned types
				SELECT 1 FROM pg_type WHERE typowner = (SELECT oid FROM pg_roles WHERE rolname = $1)
				UNION ALL
				-- Check for owned databases
				SELECT 1 FROM pg_database WHERE datdba = (SELECT oid FROM pg_roles WHERE rolname = $1)
			) owned_objects
		)`

	var ownsObjects bool
	err := c.db.QueryRow(ctx, query, roleName).Scan(&ownsObjects)
	if err != nil {
		l.Error("error checking if role owns objects", zap.Error(err))
		return false, err
	}

	return ownsObjects, nil
}

// RevokeAllGrantsFromRole revokes all grants from a role across all schemas.
func (c *Client) RevokeAllGrantsFromRole(ctx context.Context, roleName string) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()

	schemasQuery := `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg_%'
		AND nspname != 'information_schema'
		ORDER BY nspname`

	rows, err := c.db.Query(ctx, schemasQuery)
	if err != nil {
		l.Error("error querying schemas", zap.Error(err))
		return err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			l.Error("error scanning schema name", zap.Error(err))
			return err
		}
		schemas = append(schemas, schemaName)
	}

	if err := rows.Err(); err != nil {
		l.Error("error iterating schemas", zap.Error(err))
		return err
	}

	for _, schema := range schemas {
		sanitizedSchema := pgx.Identifier{schema}.Sanitize()

		revokeTablesQuery := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", sanitizedSchema, sanitizedRoleName)
		l.Debug("revoking table grants", zap.String("query", revokeTablesQuery))
		if _, err := c.db.Exec(ctx, revokeTablesQuery); err != nil {
			l.Warn("error revoking table grants", zap.String("schema", schema), zap.Error(err))
		}

		revokeSequencesQuery := fmt.Sprintf("REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s", sanitizedSchema, sanitizedRoleName)
		l.Debug("revoking sequence grants", zap.String("query", revokeSequencesQuery))
		if _, err := c.db.Exec(ctx, revokeSequencesQuery); err != nil {
			l.Warn("error revoking sequence grants", zap.String("schema", schema), zap.Error(err))
		}

		revokeFunctionsQuery := fmt.Sprintf("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %s FROM %s", sanitizedSchema, sanitizedRoleName)
		l.Debug("revoking function grants", zap.String("query", revokeFunctionsQuery))
		if _, err := c.db.Exec(ctx, revokeFunctionsQuery); err != nil {
			l.Warn("error revoking function grants", zap.String("schema", schema), zap.Error(err))
		}

		typesQuery := `
			SELECT typname 
			FROM pg_type t 
			JOIN pg_namespace n ON t.typnamespace = n.oid 
			WHERE n.nspname = $1 
			AND t.typtype = 'c'`

		typeRows, err := c.db.Query(ctx, typesQuery, schema)
		if err != nil {
			l.Warn("error querying types", zap.String("schema", schema), zap.Error(err))
		} else {
			defer typeRows.Close()

			for typeRows.Next() {
				var typeName string
				if err := typeRows.Scan(&typeName); err != nil {
					l.Warn("error scanning type name", zap.String("schema", schema), zap.Error(err))
					continue
				}

				sanitizedTypeName := pgx.Identifier{schema, typeName}.Sanitize()
				revokeTypeQuery := fmt.Sprintf("REVOKE ALL ON TYPE %s FROM %s", sanitizedTypeName, sanitizedRoleName)
				l.Debug("revoking type grants", zap.String("query", revokeTypeQuery))
				if _, err := c.db.Exec(ctx, revokeTypeQuery); err != nil {
					l.Warn("error revoking type grants", zap.String("schema", schema), zap.String("type", typeName), zap.Error(err))
				}
			}
		}

		revokeSchemaQuery := fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s", sanitizedSchema, sanitizedRoleName)
		l.Debug("revoking schema grants", zap.String("query", revokeSchemaQuery))
		if _, err := c.db.Exec(ctx, revokeSchemaQuery); err != nil {
			l.Warn("error revoking schema grants", zap.String("schema", schema), zap.Error(err))
		}
	}

	revokeDbQuery := fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM %s", pgx.Identifier{c.DatabaseName()}.Sanitize(), sanitizedRoleName)
	l.Debug("revoking database grants", zap.String("query", revokeDbQuery))
	if _, err := c.db.Exec(ctx, revokeDbQuery); err != nil {
		l.Warn("error revoking database grants", zap.Error(err))
	}

	return nil
}

// RemoveRoleFromAllRoles removes a role from all other roles.
func (c *Client) RemoveRoleFromAllRoles(ctx context.Context, roleName string) error {
	l := ctxzap.Extract(ctx)

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()

	// Get all roles that have this role as a member
	query := `
		SELECT r.rolname
		FROM pg_roles r
		JOIN pg_auth_members am ON r.oid = am.roleid
		JOIN pg_roles member ON am.member = member.oid
		WHERE member.rolname = $1`

	rows, err := c.db.Query(ctx, query, roleName)
	if err != nil {
		l.Error("error querying role memberships", zap.Error(err))
		return err
	}
	defer rows.Close()

	var parentRoles []string
	for rows.Next() {
		var parentRole string
		if err := rows.Scan(&parentRole); err != nil {
			l.Error("error scanning parent role", zap.Error(err))
			return err
		}
		parentRoles = append(parentRoles, parentRole)
	}

	if err := rows.Err(); err != nil {
		l.Error("error iterating parent roles", zap.Error(err))
		return err
	}

	// Remove the role from each parent role
	for _, parentRole := range parentRoles {
		sanitizedParentRole := pgx.Identifier{parentRole}.Sanitize()
		revokeQuery := fmt.Sprintf("REVOKE %s FROM %s", sanitizedParentRole, sanitizedRoleName)

		l.Debug("removing role from parent role", zap.String("query", revokeQuery))
		if _, err := c.db.Exec(ctx, revokeQuery); err != nil {
			l.Error("error removing role from parent role", zap.String("parent_role", parentRole), zap.Error(err))
			return err
		}
	}

	return nil
}

// SafeDeleteRole safely deletes a role by first revoking grants and removing memberships.
func (c *Client) SafeDeleteRole(ctx context.Context, roleName string) error {
	l := ctxzap.Extract(ctx)

	if roleName == "" {
		return errors.New("role name cannot be empty")
	}

	ownsObjects, err := c.RoleOwnsObjects(ctx, roleName)
	if err != nil {
		l.Error("error checking if role owns objects", zap.Error(err))
		return err
	}

	if ownsObjects {
		return fmt.Errorf("cannot delete role '%s': role owns database objects (tables, schemas, functions, etc.). Please transfer ownership or drop objects first", roleName)
	}

	l.Debug("revoking all grants from role", zap.String("role", roleName))
	if err := c.RevokeAllGrantsFromRole(ctx, roleName); err != nil {
		l.Error("error revoking grants from role", zap.Error(err))
		return err
	}

	l.Debug("removing role from all parent roles", zap.String("role", roleName))
	if err := c.RemoveRoleFromAllRoles(ctx, roleName); err != nil {
		l.Error("error removing role from parent roles", zap.Error(err))
		return err
	}

	sanitizedRoleName := pgx.Identifier{roleName}.Sanitize()
	query := "DROP ROLE " + sanitizedRoleName
	l.Debug("dropping role", zap.String("query", query))
	_, err = c.db.Exec(ctx, query)
	if err != nil {
		l.Error("error dropping role", zap.Error(err))
		return err
	}

	l.Info("successfully deleted role", zap.String("role", roleName))
	return nil
}

func (c *Client) DeleteRole(ctx context.Context, roleName string) error {
	return c.SafeDeleteRole(ctx, roleName)
}

func (c *Client) CreateUser(ctx context.Context, login string, password string) (*RoleModel, error) {
	l := ctxzap.Extract(ctx)

	if login == "" {
		return nil, errors.New("login cannot be empty")
	}
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}

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

	if userName == "" {
		return nil, errors.New("user name cannot be empty")
	}
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}

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
