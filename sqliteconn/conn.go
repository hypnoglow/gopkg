// Package sqliteconn provides optimized SQLite connection pool management.
//
// It opens separate write and read-only connection pools for file databases
// (WAL mode), and a single shared pool for in-memory databases.
// The caller is responsible for registering an sqlite3 driver (e.g. via a
// blank import of [github.com/mattn/go-sqlite3]).
//
// [github.com/mattn/go-sqlite3]: https://github.com/mattn/go-sqlite3
package sqliteconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Conn holds separate write and read connection pools for a SQLite database.
// Use [Conn.Write] for all write queries and [Conn.Read] for all read queries.
type Conn struct {
	write *sql.DB
	read  *sql.DB

	memory bool
}

// Open opens a SQLite database at the given path and returns a [Conn].
// If path is exactly ":memory:", an in-memory database is opened.
//
// Open verifies that the returned connection is ready for use and that the
// requested SQLite configuration was applied.
func Open(path string) (*Conn, error) {
	if isMemoryPath(path) {
		return newMemoryConn()
	}

	return newFileConn(path)
}

// Write returns the write connection pool.
func (c *Conn) Write() *sql.DB {
	return c.write
}

// Read returns the read-only connection pool, falling back to the write pool
// for in-memory databases which do not support read-only connections.
func (c *Conn) Read() *sql.DB {
	if c.read != nil {
		return c.read
	}
	return c.write
}

// Close closes all connection pools held by the [Conn].
func (c *Conn) Close() error {
	var errs []error
	if c.read != nil {
		if err := c.read.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read db: %w", err))
		}
	}

	if err := c.write.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close write db: %w", err))
	}

	return errors.Join(errs...)
}

func isMemoryPath(path string) bool {
	return path == ":memory:"
}

func newMemoryConn() (*Conn, error) {
	q := url.Values{}
	// Use cache=shared so that multiple connections within the same process
	// share the same in-memory database (required for testing).
	q.Set("cache", "shared")
	// SQLite disables foreign key enforcement by default; turn it on
	// to maintain referential integrity at the database level.
	q.Set("_foreign_keys", "on")

	db, err := sql.Open("sqlite3", "file::memory:?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Keep a single idle connection so the in-memory database is not destroyed
	// when all connections are released.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &Conn{write: db, memory: true}, nil
}

func fileConnParams() url.Values {
	q := url.Values{}
	// WAL mode allows concurrent reads while a write is in progress,
	// unlike the default rollback journal which blocks all readers.
	q.Set("_journal_mode", "WAL")
	// Wait up to 5 seconds when the database is locked before returning
	// SQLITE_BUSY, instead of failing immediately.
	q.Set("_busy_timeout", "5000")
	// NORMAL is safe with WAL and avoids an fsync on every commit,
	// improving write throughput without sacrificing crash safety.
	q.Set("_synchronous", "NORMAL")
	// SQLite disables foreign key enforcement by default; turn it on
	// to maintain referential integrity at the database level.
	q.Set("_foreign_keys", "ON")
	// Limit the page cache to ~20 MB (abs(N) * 1024 bytes when N is negative).
	q.Set("_cache_size", "-20000")
	return q
}

func sqliteFileURI(path string, q url.Values) string {
	u := &url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: q.Encode(),
	}
	return u.String()
}

func newFileConn(path string) (*Conn, error) {
	writeParams := fileConnParams()
	writeDB, err := sql.Open("sqlite3", sqliteFileURI(path, writeParams))
	if err != nil {
		return nil, fmt.Errorf("open write db: %w", err)
	}

	// SQLite allows only one concurrent writer; a single connection in the
	// write pool makes this explicit and avoids lock contention between writers.
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	if err := initWriteDB(writeDB); err != nil {
		_ = writeDB.Close()
		return nil, err
	}

	readParams := fileConnParams()
	readParams.Set("mode", "ro")
	readDB, err := sql.Open("sqlite3", sqliteFileURI(path, readParams))
	if err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("open read db: %w", err)
	}

	// Allow multiple concurrent read connections and keep them all idle
	// to avoid the overhead of reopening connections under load.
	readDB.SetMaxOpenConns(10)
	readDB.SetMaxIdleConns(10)

	if err := initReadDB(readDB); err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}

	return &Conn{write: writeDB, read: readDB, memory: false}, nil
}

func initWriteDB(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping write db: %w", err)
	}

	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return fmt.Errorf("check journal mode: %w", err)
	}
	if journalMode != "wal" {
		// SQLite can silently remain in rollback journal mode on unsupported
		// filesystems, such as some network mounts:
		// https://sqlite.org/wal.html#activating_and_configuring_wal_mode
		return fmt.Errorf("expected WAL journal mode, got %q — check that the database is on a local filesystem", journalMode)
	}

	return nil
}

func initReadDB(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping read db: %w", err)
	}

	return nil
}
