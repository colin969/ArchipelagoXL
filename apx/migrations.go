package main

import (
	"database/sql"
	"fmt"
	"time"
)

var migrations = []struct {
	version int
	sql     string
}{
	{1, `
        CREATE TABLE IF NOT EXISTS rooms (
            lobby_room_id         TEXT PRIMARY KEY,
            normal_port           INTEGER NOT NULL,
            reduced_port          INTEGER NOT NULL,
            created_at            BIGINT NOT NULL,
            disabled              BOOLEAN NOT NULL DEFAULT FALSE,
            per_slot_passwords    BOOLEAN NOT NULL DEFAULT FALSE,
            deathlink_disabled    BOOLEAN NOT NULL DEFAULT FALSE,
            reduced_access        BOOLEAN NOT NULL DEFAULT FALSE,
            deathlink_probability DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            deaths_sent           INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS room_full_feed (
            lobby_room_id TEXT NOT NULL,
            slot_id       INTEGER NOT NULL,
            PRIMARY KEY (lobby_room_id, slot_id)
        );
        CREATE TABLE IF NOT EXISTS room_slot_bounce_exclusions (
            lobby_room_id TEXT NOT NULL,
            slot_id       INTEGER NOT NULL,
            PRIMARY KEY (lobby_room_id, slot_id)
        );
        CREATE TABLE IF NOT EXISTS room_bounce_tag_exclusions (
            lobby_room_id TEXT NOT NULL,
            slot_id       INTEGER NOT NULL,
            tag           TEXT NOT NULL,
            PRIMARY KEY (lobby_room_id, slot_id, tag)
        );
        CREATE TABLE IF NOT EXISTS room_alt_connect_names (
            lobby_room_id TEXT NOT NULL,
            alt_name      TEXT NOT NULL,
            slot_name     TEXT NOT NULL,
            PRIMARY KEY (lobby_room_id, alt_name, slot_name)
        );
    `},
	{2, `
        CREATE TABLE IF NOT EXISTS room_slot_deaths (
            lobby_room_id TEXT NOT NULL,
            slot_id       INTEGER NOT NULL,
            count         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (lobby_room_id, slot_id)
        );
    `},
	{3, `
        CREATE TABLE IF NOT EXISTS saved_rooms (
            lobby_room_id    TEXT PRIMARY KEY,
            state            JSONB NOT NULL
        );
    `},
	{4, `
				ALTER TABLE room_full_feed
						ADD CONSTRAINT fk_room_full_feed_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;

				ALTER TABLE room_slot_bounce_exclusions
						ADD CONSTRAINT fk_room_slot_bounce_exclusions_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;

				ALTER TABLE room_bounce_tag_exclusions
						ADD CONSTRAINT fk_room_bounce_tag_exclusions_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;

				ALTER TABLE room_alt_connect_names
						ADD CONSTRAINT fk_room_alt_connect_names_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;

				ALTER TABLE room_slot_deaths
						ADD CONSTRAINT fk_room_slot_deaths_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;

				ALTER TABLE saved_rooms
						ADD CONSTRAINT fk_saved_rooms_room
						FOREIGN KEY (lobby_room_id) REFERENCES rooms(lobby_room_id) ON DELETE CASCADE;
	`},
	{6, `
		ALTER TABLE rooms ADD COLUMN IF NOT EXISTS multidata BYTEA;
	`},
	{7, `
    DELETE FROM rooms WHERE multidata IS NULL;
	`},
}

func runMigrations(db *sql.DB) error {
	if _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at BIGINT NOT NULL
        );
    `); err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	for _, m := range migrations {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = $1`, m.version).Scan(&count); err != nil {
			return fmt.Errorf("checking migration %d: %w", m.version, err)
		}
		if count > 0 {
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("migration %d: begin tx: %w", m.version, err)
		}

		if _, err := tx.Exec(m.sql); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", m.version, err)
		}

		if _, err := tx.Exec(
			`INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)`,
			m.version, time.Now().Unix(),
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: recording version: %w", m.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %d: commit: %w", m.version, err)
		}
	}

	db.Exec(`DELETE FROM rooms WHERE multidata IS NULL`)

	return nil
}
