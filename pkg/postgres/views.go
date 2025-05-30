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

type ViewModel struct {
	ID      int64    `db:"oid"`
	Name    string   `db:"relname"`
	Schema  string   `db:"nspname"`
	OwnerID int64    `db:"relowner"`
	ACLs    []string `db:"relacl"`
}

func (t *ViewModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *ViewModel) GetACLs() []string {
	return t.ACLs
}

func (t *ViewModel) AllPrivileges() PrivilegeSet {
	return Insert | Select | Update | Delete | Truncate | References | Trigger
}

func (t *ViewModel) DefaultPrivileges() PrivilegeSet {
	return EmptyPrivilegeSet
}

func (c *Client) GetView(ctx context.Context, viewID int64) (*ViewModel, error) {
	ret := &ViewModel{}

	q := c.getClassQuery(ctx)

	err := pgxscan.Get(ctx, c.db, ret, q, viewID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListViews(ctx context.Context, schemaID int64, pager *Pager) ([]*ViewModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing views")

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}

	_, _ = sb.WriteString(`
SELECT c."oid"::int, c."relname", c."relowner", n."nspname", c."relacl"
FROM pg_class c
         LEFT JOIN pg_namespace n ON n."oid" = c."relnamespace"
WHERE n."oid" = $1 AND c."relkind" = 'v'
`)
	args = append(args, schemaID)

	_, _ = sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*ViewModel
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

func (c *Client) GrantView(ctx context.Context, schema, viewName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("granting view", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedViewName := pgx.Identifier{viewName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := pgx.Identifier{transformPrivilege(privilege)}.Sanitize()

	q := fmt.Sprintf("GRANT %s ON %s.%s TO %s", sanitizedPrivilege, sanitizedSchema, sanitizedViewName, sanitizedPrincipalName)

	if isGrant {
		q += " WITH GRANT OPTION"
	}

	_, err := c.db.Exec(ctx, q)
	return err
}

func (c *Client) RevokeView(ctx context.Context, schema, viewName string, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking view", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedViewName := pgx.Identifier{viewName}.Sanitize()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := pgx.Identifier{transformPrivilege(privilege)}.Sanitize()

	var q string

	if isGrant {
		q = fmt.Sprintf("REVOKE GRANT OPTION for %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedViewName, sanitizedPrincipalName)
	} else {
		q = fmt.Sprintf("REVOKE %s ON TABLE %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedViewName, sanitizedPrincipalName)
	}

	_, err := c.db.Exec(ctx, q)
	return err
}
