# gopkg

A collection of small, focused Go packages. Each package lives in its own
subdirectory and is published as an independent Go module, so you only pull in
what you actually use.

## Packages

| Module | Import path | Description |
|--------|-------------|-------------|
| [sqliteconn](https://pkg.go.dev/github.com/hypnoglow/gopkg/sqliteconn) | `github.com/hypnoglow/gopkg/sqliteconn` | SQLite connection pool management (WAL, separate read/write pools) |

## Development

```bash
task default
```
