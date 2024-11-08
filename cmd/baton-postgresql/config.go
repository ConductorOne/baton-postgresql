package main

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	dsn                 = field.StringField("dsn", field.WithRequired(true), field.WithDescription("The DSN to connect to the database"))
	schemas             = field.StringSliceField("schemas", field.WithDescription("The schemas to include in the sync"))
	includeColumns      = field.BoolField("include-columns", field.WithDescription("Include column privileges when syncing. This can result in large amounts of data"))
	includeLargeObjects = field.BoolField("include-large-objects", field.WithDescription("Include large objects when syncing. This can result in large amounts of data"))
)

var relationships = []field.SchemaFieldRelationship{}

var configuration = field.NewConfiguration([]field.SchemaField{
	dsn, schemas, includeColumns, includeLargeObjects,
}, relationships...)
