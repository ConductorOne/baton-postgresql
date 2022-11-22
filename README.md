# baton-postgresql

## Usage
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
  -f, --file string         The path to the c1z file to sync with ($C1_FILE) (default "sync.c1z")
  -h, --help                help for baton-postgresql
      --log-format string   The output format for logs: json, console ($C1_LOG_FORMAT) (default "json")
      --log-level string    The log level: debug, info, warn, error ($C1_LOG_LEVEL) (default "info")
      --schemas strings     The schemas to include in the sync. This defaults to 'public' only. (default [public])
  -v, --version             version for baton-postgresql

Use "baton-postgresql [command] --help" for more information about a command.
```