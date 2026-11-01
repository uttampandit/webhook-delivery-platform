package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var migrationFilename = regexp.MustCompile(`^([0-9]+)_(.+)\.sql$`)

type migration struct {
	version int64
	name    string
	sha256  string
	sql     string
}

func readMigrations(dir string) ([]migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migration directory %s: %w", dir, err)
	}

	migrations := make([]migration, 0, len(entries))
	seen := make(map[int64]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		match := migrationFilename.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("invalid migration filename %q; expected <number>_<name>.sql", entry.Name())
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration version in filename %q", entry.Name())
		}
		if previous, ok := seen[version]; ok {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, entry.Name())
		}
		seen[version] = entry.Name()

		path := filepath.Join(dir, entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat migration %s: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("migration %s is not a regular file", entry.Name())
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(contents)
		migrations = append(migrations, migration{
			version: version,
			name:    entry.Name(),
			sha256:  hex.EncodeToString(sum[:]),
			sql:     string(contents),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	for i, migration := range migrations {
		if strings.TrimSpace(migration.sql) == "" {
			return nil, fmt.Errorf("migration %s is empty", migration.name)
		}
		if i > 0 && migrations[i-1].version >= migration.version {
			return nil, fmt.Errorf("migration versions must be strictly increasing")
		}
	}
	return migrations, nil
}
