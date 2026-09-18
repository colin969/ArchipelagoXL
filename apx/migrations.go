package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

var migrations = []struct {
	version int
	sql     string
}{
	{1, `
			CREATE TABLE IF NOT EXISTS rooms (
					lobby_room_id         TEXT PRIMARY KEY,
					ap_room_id            TEXT NOT NULL,
					normal_port           INTEGER NOT NULL,
					reduced_port          INTEGER NOT NULL,
					created_at            INTEGER NOT NULL,
					disabled              INTEGER NOT NULL DEFAULT 0,
					per_slot_passwords    INTEGER NOT NULL DEFAULT 0,
					deathlink_disabled    INTEGER NOT NULL DEFAULT 0,
					reduced_access        INTEGER NOT NULL DEFAULT 0,
					deathlink_probability REAL NOT NULL DEFAULT 1.0,
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
}

func runMigrations(db *sql.DB) error {
	if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS schema_migrations (
					version    INTEGER PRIMARY KEY,
					applied_at INTEGER NOT NULL
			);
	`); err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	for _, m := range migrations {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, m.version).Scan(&count); err != nil {
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
			`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
			m.version, time.Now().Unix(),
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: recording version: %w", m.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %d: commit: %w", m.version, err)
		}

		log.Printf("applied migration %d", m.version)
	}

	return nil
}
