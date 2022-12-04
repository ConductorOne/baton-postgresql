![Baton Logo](./docs/images/baton-logo.png)

# `baton-postgresql` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-postgresql.svg)](https://pkg.go.dev/github.com/conductorone/baton-postgresql) ![main ci](https://github.com/conductorone/baton-postgresql/actions/workflows/main.yaml/badge.svg)

`baton-postgresql` is a connector for PostgreSQL built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It connects to a PostgreSQL database and syncs data about which roles have access to which resources within the database.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Getting Started

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-postgresql

baton-postgresql --dsn "postgres://username:password@localhost:5432/database_name"
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_DSN=postgres://username:password@localhost:5432/database_name ghcr.io/conductorone/baton-postgresql:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-postgresql/cmd/baton-postgresql@main

baton-postgresql --dsn "postgres://username:password@localhost:5432/database_name"
baton resources
```

#

`baton-postgresql` will sync information about the following PostgreSQL resources:

- Roles
- Databases
- Schemas
- Functions/Procedures
- Tables/Views
- Sequences
- Columns
- Large Objects

By default, `baton-postgresql` will only sync information from the `public` schema. You can use the `--schemas` flag to specify other schemas.

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-postgresql` Command Line Usage

```
baton-postgresql

Usage:
  baton-postgresql [flags]
  baton-postgresql [command]

Available Commands:
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --dsn string          The connection string for the PostgreSQL database ($BATON_DSN)
                            example: postgres://username:password@localhost:5432/database_name
  -f, --file string         The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                help for baton-postgresql
      --log-format string   The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string    The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --schemas strings     The schemas to include in the sync. ($BATON_SCHEMAS)
                            This defaults to 'public' only. (default [public])
  -v, --version             version for baton-postgresql

Use "baton-postgresql [command] --help" for more information about a command.
```
