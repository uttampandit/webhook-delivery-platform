package store

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestReadMigrationsSortsAndHashes(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"002_second.sql": "SELECT 2;\n",
		"001_first.sql":  "SELECT 1;\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	migrations, err := readMigrations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 2 || migrations[0].version != 1 || migrations[1].version != 2 {
		t.Fatalf("migration order = %#v; want versions [1 2]", migrations)
	}
	want := sha256.Sum256([]byte(files["001_first.sql"]))
	if migrations[0].sha256 != hex.EncodeToString(want[:]) {
		t.Fatalf("sha256 = %q; want %q", migrations[0].sha256, hex.EncodeToString(want[:]))
	}
}

func TestReadMigrationsRejectsInvalidAndDuplicateVersions(t *testing.T) {
	t.Run("invalid filename", func(t *testing.T) {
		dir := t.TempDir()
		writeMigrationFile(t, dir, "first.sql", "SELECT 1;")
		if _, err := readMigrations(dir); err == nil {
			t.Fatal("readMigrations accepted an unnumbered SQL file")
		}
	})

	t.Run("duplicate version", func(t *testing.T) {
		dir := t.TempDir()
		writeMigrationFile(t, dir, "001_first.sql", "SELECT 1;")
		writeMigrationFile(t, dir, "001_second.sql", "SELECT 2;")
		if _, err := readMigrations(dir); err == nil {
			t.Fatal("readMigrations accepted duplicate migration versions")
		}
	})

	t.Run("empty migration", func(t *testing.T) {
		dir := t.TempDir()
		writeMigrationFile(t, dir, "001_empty.sql", " \n\t\n")
		if _, err := readMigrations(dir); err == nil {
			t.Fatal("readMigrations accepted an empty migration")
		}
	})
}

func writeMigrationFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
