package store

import "testing"

func TestLoadMigrations(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations() error = %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("loadMigrations() returned no migrations")
	}
	if migrations[0].name != "0001_init.sql" || migrations[0].version != 1 {
		t.Fatalf("first migration = %q (version %d), want 0001_init.sql (version 1)",
			migrations[0].name, migrations[0].version)
	}
	if migrations[0].sql == "" {
		t.Fatal("first migration has empty SQL")
	}
	for i := 1; i < len(migrations); i++ {
		if migrations[i].version <= migrations[i-1].version {
			t.Fatalf("migration versions are not strictly ascending: %d then %d",
				migrations[i-1].version, migrations[i].version)
		}
	}
}

func TestParseMigrationName(t *testing.T) {
	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{name: "0001_init.sql", want: 1},
		{name: "0012_add_index.sql", want: 12},
		{name: "init.sql", wantErr: true},
		{name: "_init.sql", wantErr: true},
		{name: "0_init.sql", wantErr: true},
		{name: "-1_init.sql", wantErr: true},
		{name: "x_init.sql", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseMigrationName(tt.name)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("parseMigrationName(%q) succeeded, want error", tt.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseMigrationName(%q) error = %v", tt.name, err)
		}
		if got != tt.want {
			t.Fatalf("parseMigrationName(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestDSNHostDoesNotExposePassword(t *testing.T) {
	got := dsnHost("postgres://relayx:s3cret@127.0.0.1:5432/relayx?sslmode=disable")
	if got != "127.0.0.1:5432" {
		t.Fatalf("dsnHost() = %q, want 127.0.0.1:5432", got)
	}
}
