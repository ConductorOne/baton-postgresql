package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type FunctionModel struct {
	ID         int64    `db:"oid"`
	Name       string   `db:"proname"`
	Schema     string   `db:"nspname"`
	OwnerID    int64    `db:"proowner"`
	ACLs       []string `db:"proacl"`
	Arguments  string   `db:"arguments"`
	ReturnType string   `db:"return_type"`
}

func (t *FunctionModel) GetOwnerID() int64 {
	return t.OwnerID
}

func (t *FunctionModel) GetACLs() []string {
	return t.ACLs
}

func (t *FunctionModel) AllPrivileges() PrivilegeSet {
	return Execute
}

func (t *FunctionModel) DefaultPrivileges() PrivilegeSet {
	return Execute
}

func (t *FunctionModel) Signature() string {
	return fmt.Sprintf("%s(%s)", t.Name, t.Arguments)
}

func (c *Client) GetFunction(ctx context.Context, functionID int64) (*FunctionModel, error) {
	ret := &FunctionModel{}

	q := `
SELECT DISTINCT 
       a."oid"::int,
       a."proname",
       n."nspname",
       a."proowner"::int, a."proacl",
	   pg_get_function_arguments(a.oid) AS arguments,
       pg_get_function_result(a.oid) AS return_type
FROM "pg_catalog"."pg_proc" a
         LEFT JOIN pg_namespace n ON n."oid" = a."pronamespace"
WHERE a."oid" = $1
`

	err := pgxscan.Get(ctx, c.db, ret, q, functionID)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) ListFunctions(ctx context.Context, schemaID int64, skipBuiltInFunctions bool, pager *Pager) ([]*FunctionModel, string, error) {
	l := ctxzap.Extract(ctx)
	l.Debug("listing functions for schema", zap.Int64("schema_id", schemaID))

	offset, limit, err := pager.Parse()
	if err != nil {
		return nil, "", err
	}
	var args []interface{}
	sb := &strings.Builder{}
	_, _ = sb.WriteString(`
SELECT a."oid"::int,
       a."proname",
       n."nspname",
       a."proowner"::int, a."proacl",
	   pg_get_function_arguments(a.oid) AS arguments,
       pg_get_function_result(a.oid) AS return_type
FROM "pg_catalog"."pg_proc" a
         LEFT JOIN pg_namespace n ON n."oid" = a."pronamespace"
WHERE a."prokind" = 'f'
  AND a."pronamespace" = $1
`)

	if skipBuiltInFunctions {
		_, _ = sb.WriteString(` AND n."nspname" NOT IN ('pg_catalog', 'information_schema') `)
	}

	args = append(args, schemaID)
	_, _ = sb.WriteString("LIMIT $2 ")
	args = append(args, limit+1)
	if offset > 0 {
		_, _ = sb.WriteString("OFFSET $3")
		args = append(args, offset)
	}

	var ret []*FunctionModel
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

func (c *Client) GrantFunction(ctx context.Context, schema string, functionSignature *FunctionModel, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("granting function", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedFunctionSignature := functionSignature.Signature()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)

	q := fmt.Sprintf("GRANT %s ON FUNCTION %s.%s TO %s", sanitizedPrivilege, sanitizedSchema, sanitizedFunctionSignature, sanitizedPrincipalName)

	if isGrant {
		q += withGrantOptions
	}

	_, err := c.db.Exec(ctx, q)
	return err
}

func (c *Client) RevokeFunction(ctx context.Context, schema string, functionSignature *FunctionModel, principalName string, privilege string, isGrant bool) error {
	l := ctxzap.Extract(ctx)
	l.Debug("revoking function", zap.String("principalName", principalName), zap.String("privilege", privilege))

	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	sanitizedFunctionSignature := functionSignature.Signature()
	sanitizedPrincipalName := pgx.Identifier{principalName}.Sanitize()
	sanitizedPrivilege := sanitizePrivilege(privilege)
	var q string

	if isGrant {
		q = fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON FUNCTION %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedFunctionSignature, sanitizedPrincipalName)
	} else {
		q = fmt.Sprintf("REVOKE %s ON FUNCTION %s.%s FROM %s", sanitizedPrivilege, sanitizedSchema, sanitizedFunctionSignature, sanitizedPrincipalName)
	}

	_, err := c.db.Exec(ctx, q)
	return err
}
