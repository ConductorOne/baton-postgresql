package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type TableModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"relname"`
	Schema  string   `db:"nspname"`
	OwnerID int64    `db:"relowner"`
	ACLs    []string `db:"relacl"`
}

func (t *TableModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *TableModel) GetACLs() []string {
	return t.ACLs
}

func (t *TableModel) AllPrivileges() PrivilegeSet {
	return Insert | Select | Update | Delete | Truncate | References | Trigger
}

func (t *TableModel) DefaultPrivileges() PrivilegeSet {
	return EmptyPrivilegeSet
}

func (c *Client) GetTable(ctx context.Context, tableID int64) (*TableModel, error) {
	ret := &TableModel{}

	q := c.getClassQuery(ctx)

	err := pgxscan.Get(ctx, c.db, ret, q, tableID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListTables(ctx context.Context, schemaName string, pager *Pager) ([]*TableModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing tables")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT c."oid"::int, c."relname", c."relowner"::int, n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE n."nspname" = $1
  AND (c."relkind" = 'r' OR c."relkind" = 'p')
`)

	args = append(args, schemaName)
	_, _ = sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*TableModel
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

func (c *Client) GrantTable(ctx context.Context, schema string, tableName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("granting table", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedTableName := pgx.Identifier{tableName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)

	q := fmt.Sprintf("GRANT %s ON TABLE %s.%s TO %s", sanitizedPrivilege, sanitizedSchema, sanitizedTableName, sanitizedPrincipalName)

	if isGrant {
		q += withGrantOptions
	}

	_, err := c.db.Exec(ctx, q)
	return err
}

func (c *Client) RevokeTable(ctx context.Context, schema string, tableName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking table", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedTableName := pgx.Identifier{tableName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)

	var q string

	if isGrant {
		q = fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedTableName, sanitizedPrincipalName)
	} else {
		q = fmt.Sprintf("REVOKE %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedTableName, sanitizedPrincipalName)
	}

	_, err := c.db.Exec(ctx, q)
	return err
}
