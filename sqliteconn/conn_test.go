package sqliteconn

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Open must fully initialize a fresh file database before returning, so the
// first operation through the read pool succeeds without requiring a prior
// write.
func TestOpen_NewFile_FirstReadWorks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database.sqlite")

	conn, err := Open(path)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
	})

	ctx := context.Background()
	var n int
	err = conn.Read().QueryRowContext(ctx, "SELECT 1").Scan(&n)
	if err != nil {
		t.Fatalf("first read from a fresh file database failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("got %d, want 1", n)
	}
}

func TestOpen_Memory_UsesSharedPoolAndEnforcesForeignKeys(t *testing.T) {
	t.Parallel()

	conn, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
	})

	if conn.Read() != conn.Write() {
		t.Fatalf("memory db must use a shared pool for reads and writes")
	}

	ctx := context.Background()
	mustExec(t, conn.Write(), ctx, `
		CREATE TABLE parent (
			id INTEGER PRIMARY KEY
		);
	`)
	mustExec(t, conn.Write(), ctx, `
		CREATE TABLE child (
			id INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL REFERENCES parent(id)
		);
	`)

	_, err = conn.Write().ExecContext(ctx, `INSERT INTO child (id, parent_id) VALUES (1, 999)`)
	if err == nil {
		t.Fatalf("expected foreign key violation, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Fatalf("unexpected foreign key error: %v", err)
	}

	mustExec(t, conn.Write(), ctx, `INSERT INTO parent (id) VALUES (1)`)
	mustExec(t, conn.Write(), ctx, `INSERT INTO child (id, parent_id) VALUES (1, 1)`)

	var count int
	if err := conn.Read().QueryRowContext(ctx, `SELECT COUNT(*) FROM child`).Scan(&count); err != nil {
		t.Fatalf("count child rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d child rows, want 1", count)
	}
}

func TestOpen_File_UsesSeparateReadPoolAndReadOnlyReads(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database.sqlite")

	conn, err := Open(path)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
	})

	if conn.Read() == conn.Write() {
		t.Fatalf("file db must use separate read and write pools")
	}

	ctx := context.Background()
	mustExec(t, conn.Write(), ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	mustExec(t, conn.Write(), ctx, `INSERT INTO items (id, name) VALUES (1, 'alpha')`)

	var name string
	if err := conn.Read().QueryRowContext(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("got %q, want %q", name, "alpha")
	}

	_, err = conn.Read().ExecContext(ctx, `INSERT INTO items (id, name) VALUES (2, 'beta')`)
	if err == nil {
		t.Fatalf("expected write through read-only pool to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "readonly") {
		t.Fatalf("unexpected read-only error: %v", err)
	}
}

func TestOpen_File_AppliesPragmas(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database.sqlite")

	conn, err := Open(path)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
	})

	ctx := context.Background()

	if got := pragmaString(t, conn.Write(), ctx, "journal_mode"); got != "wal" {
		t.Fatalf("write journal_mode = %q, want %q", got, "wal")
	}
	if got := pragmaInt(t, conn.Write(), ctx, "foreign_keys"); got != 1 {
		t.Fatalf("write foreign_keys = %d, want 1", got)
	}
	if got := pragmaInt(t, conn.Read(), ctx, "foreign_keys"); got != 1 {
		t.Fatalf("read foreign_keys = %d, want 1", got)
	}
	if got := pragmaInt(t, conn.Write(), ctx, "busy_timeout"); got != 5000 {
		t.Fatalf("write busy_timeout = %d, want 5000", got)
	}
	if got := pragmaInt(t, conn.Read(), ctx, "busy_timeout"); got != 5000 {
		t.Fatalf("read busy_timeout = %d, want 5000", got)
	}
}

func TestOpen_File_PathWithURISpecialChars_UsesExactPath(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database?name#fragment.sqlite")

	conn, err := Open(path)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
	})

	ctx := context.Background()
	mustExec(t, conn.Write(), ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	mustExec(t, conn.Write(), ctx, `INSERT INTO items (id, name) VALUES (1, 'alpha')`)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected database file at exact path %q: %v", path, err)
	}
}

func Test_initWriteDB_RejectsNonWALMode(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database.sqlite")

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	ctx := context.Background()
	mustExec(t, db, ctx, `PRAGMA journal_mode=DELETE`)

	err = initWriteDB(db)
	if err == nil {
		t.Fatalf("expected non-WAL mode error, got nil")
	}
	if !strings.Contains(err.Error(), `expected WAL journal mode, got "delete"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_InvalidPathReturnsError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing", "database.sqlite")

	_, err := Open(path)
	if err == nil {
		t.Fatalf("expected open with invalid path to fail")
	}
	if !strings.Contains(err.Error(), "ping write db") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConn_Close_ClosesReadAndWritePools(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "database.sqlite")

	conn, err := Open(path)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}

	ctx := context.Background()
	if err := conn.Write().PingContext(ctx); err == nil {
		t.Fatalf("expected closed write pool to fail ping")
	}
	if err := conn.Read().PingContext(ctx); err == nil {
		t.Fatalf("expected closed read pool to fail ping")
	}
}

func mustExec(t *testing.T, db *sql.DB, ctx context.Context, query string) {
	t.Helper()

	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func pragmaInt(t *testing.T, db *sql.DB, ctx context.Context, name string) int {
	t.Helper()

	var value int
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
		t.Fatalf("query PRAGMA %s: %v", name, err)
	}
	return value
}

func pragmaString(t *testing.T, db *sql.DB, ctx context.Context, name string) string {
	t.Helper()

	var value string
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
		t.Fatalf("query PRAGMA %s: %v", name, err)
	}
	return strings.ToLower(value)
}
