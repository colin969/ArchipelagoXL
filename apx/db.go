package main

import (
	"database/sql"
	"fmt"
	"time"
)

type RoomStore struct {
	db *sql.DB
}

type RoomRecord struct {
	LobbyRoomId          string
	ApRoomId             string
	NormalId             int
	ReducedId            int
	CreatedAt            time.Time
	Disabled             bool
	PerSlotPasswords     bool
	DeathlinkDisabled    bool
	ReducedAccess        bool
	DeathlinkProbability float64
}

func NewRoomStore(path string) (*RoomStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite: %w", err)
	}
	err = runMigrations(db)
	if err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}
	return &RoomStore{db: db}, nil
}

func (s *RoomStore) Disable(lobbyRoomId string) error {
	_, err := s.db.Exec(`UPDATE rooms SET disabled = 1 WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

func (s *RoomStore) Save(r RoomRecord) error {
	_, err := s.db.Exec(`
			INSERT INTO rooms (lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, per_slot_passwords, deathlink_disabled, reduced_access)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(lobby_room_id) DO UPDATE SET
					ap_room_id         = excluded.ap_room_id,
					normal_port        = excluded.normal_port,
					reduced_port       = excluded.reduced_port,
					per_slot_passwords = excluded.per_slot_passwords,
					deathlink_disabled = excluded.deathlink_disabled,
					reduced_access     = excluded.reduced_access
	`, r.LobbyRoomId, r.ApRoomId, r.NormalId, r.ReducedId, r.CreatedAt.Unix(), r.PerSlotPasswords, r.DeathlinkDisabled, r.ReducedAccess)
	return err
}

func (s *RoomStore) Delete(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM rooms WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

// Deathlink probability

func (s *RoomStore) SaveDeathlinkProbability(lobbyRoomId string, prob float64) error {
	_, err := s.db.Exec(
		`UPDATE rooms SET deathlink_probability = ? WHERE lobby_room_id = ?`,
		prob, lobbyRoomId,
	)
	return err
}

// Normal access / Full feed slots

func (s *RoomStore) SetFullFeedSlot(lobbyRoomId string, slotId int) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO room_full_feed (lobby_room_id, slot_id) VALUES (?, ?)`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) DeleteFullFeedSlot(lobbyRoomId string, slotId int) error {
	_, err := s.db.Exec(
		`DELETE FROM room_full_feed WHERE lobby_room_id = ? AND slot_id = ?`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) SaveFullFeedSlots(lobbyRoomId string, slotIds []int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM room_full_feed WHERE lobby_room_id = ?`, lobbyRoomId); err != nil {
		tx.Rollback()
		return err
	}
	for _, id := range slotIds {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO room_full_feed (lobby_room_id, slot_id) VALUES (?, ?)`,
			lobbyRoomId, id,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *RoomStore) LoadFullFeedSlots(lobbyRoomId string) ([]int, error) {
	rows, err := s.db.Query(
		`SELECT slot_id FROM room_full_feed WHERE lobby_room_id = ?`, lobbyRoomId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *RoomStore) DeleteFullFeedSlots(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM room_full_feed WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

// Slot isolation / bounce exclusions

func (s *RoomStore) AddSlotBounceExclusion(lobbyRoomId string, slotId int) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO room_slot_bounce_exclusions (lobby_room_id, slot_id) VALUES (?, ?)`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) RemoveSlotBounceExclusion(lobbyRoomId string, slotId int) error {
	_, err := s.db.Exec(
		`DELETE FROM room_slot_bounce_exclusions WHERE lobby_room_id = ? AND slot_id = ?`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) SaveSlotBounceExclusions(lobbyRoomId string, slotIds []int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM room_slot_bounce_exclusions WHERE lobby_room_id = ?`, lobbyRoomId); err != nil {
		tx.Rollback()
		return err
	}
	for _, id := range slotIds {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO room_slot_bounce_exclusions (lobby_room_id, slot_id) VALUES (?, ?)`,
			lobbyRoomId, id,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *RoomStore) LoadSlotBounceExclusions(lobbyRoomId string) ([]int, error) {
	rows, err := s.db.Query(
		`SELECT slot_id FROM room_slot_bounce_exclusions WHERE lobby_room_id = ?`, lobbyRoomId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *RoomStore) DeleteSlotBounceExclusions(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM room_slot_bounce_exclusions WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

// Bounce tag exclusions

func (s *RoomStore) AddBounceTagExclusion(lobbyRoomId string, slotId int, tag string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO room_bounce_tag_exclusions (lobby_room_id, slot_id, tag) VALUES (?, ?, ?)`,
		lobbyRoomId, slotId, tag,
	)
	return err
}

func (s *RoomStore) RemoveBounceTagExclusion(lobbyRoomId string, slotId int, tag string) error {
	_, err := s.db.Exec(
		`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = ? AND slot_id = ? AND tag = ?`,
		lobbyRoomId, slotId, tag,
	)
	return err
}

func (s *RoomStore) SaveBounceTagExclusions(lobbyRoomId string, exclusions map[int][]string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = ?`, lobbyRoomId); err != nil {
		tx.Rollback()
		return err
	}
	for slotId, tags := range exclusions {
		for _, tag := range tags {
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO room_bounce_tag_exclusions (lobby_room_id, slot_id, tag) VALUES (?, ?, ?)`,
				lobbyRoomId, slotId, tag,
			); err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *RoomStore) LoadBounceTagExclusions(lobbyRoomId string) (map[int][]string, error) {
	rows, err := s.db.Query(
		`SELECT slot_id, tag FROM room_bounce_tag_exclusions WHERE lobby_room_id = ?`, lobbyRoomId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int][]string)
	for rows.Next() {
		var slotId int
		var tag string
		if err := rows.Scan(&slotId, &tag); err != nil {
			return nil, err
		}
		result[slotId] = append(result[slotId], tag)
	}
	return result, rows.Err()
}

func (s *RoomStore) DeleteBounceTagExclusions(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

func (s *RoomStore) DeleteBounceTagExclusionsForTag(lobbyRoomId string, tag string) error {
	_, err := s.db.Exec(
		`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = ? AND tag = ?`,
		lobbyRoomId, tag,
	)
	return err
}

// Alt connect names

func (s *RoomStore) AddAltConnectName(lobbyRoomId, altName, slotName string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO room_alt_connect_names (lobby_room_id, alt_name, slot_name) VALUES (?, ?, ?)`,
		lobbyRoomId, altName, slotName,
	)
	return err
}

func (s *RoomStore) DeleteAltConnectName(lobbyRoomId, altName string) error {
	_, err := s.db.Exec(
		`DELETE FROM room_alt_connect_names WHERE lobby_room_id = ? AND alt_name = ?`,
		lobbyRoomId, altName,
	)
	return err
}

func (s *RoomStore) SaveAltConnectNames(lobbyRoomId string, names map[string]string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM room_alt_connect_names WHERE lobby_room_id = ?`, lobbyRoomId); err != nil {
		tx.Rollback()
		return err
	}
	for altName, slotName := range names {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO room_alt_connect_names (lobby_room_id, alt_name, slot_name) VALUES (?, ?, ?)`,
			lobbyRoomId, altName, slotName,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *RoomStore) LoadAltConnectNamesBySlot(lobbyRoomId, slotName string) ([]string, error) {
	rows, err := s.db.Query(
		`SELECT alt_name FROM room_alt_connect_names WHERE lobby_room_id = ? AND slot_name = ?`,
		lobbyRoomId, slotName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var altNames []string
	for rows.Next() {
		var altName string
		if err := rows.Scan(&altName); err != nil {
			return nil, err
		}
		altNames = append(altNames, altName)
	}
	return altNames, rows.Err()
}

func (s *RoomStore) LoadAltConnectNames(lobbyRoomId string) (map[string]string, error) {
	rows, err := s.db.Query(
		`SELECT alt_name, slot_name FROM room_alt_connect_names WHERE lobby_room_id = ?`, lobbyRoomId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var altName, slotName string
		if err := rows.Scan(&altName, &slotName); err != nil {
			return nil, err
		}
		result[altName] = slotName
	}
	return result, rows.Err()
}

func (s *RoomStore) DeleteAltConnectNames(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM room_alt_connect_names WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

// Deaths

func (s *RoomStore) IncrementSlotDeathCount(lobbyRoomId string, slotId int) error {
	_, err := s.db.Exec(`
			INSERT INTO room_slot_deaths (lobby_room_id, slot_id, count)
			VALUES (?, ?, 1)
			ON CONFLICT(lobby_room_id, slot_id) DO UPDATE SET count = count + 1
	`, lobbyRoomId, slotId)
	return err
}

func (s *RoomStore) LoadSlotDeaths(lobbyRoomId string) (map[int]int, error) {
	rows, err := s.db.Query(
		`SELECT slot_id, count FROM room_slot_deaths WHERE lobby_room_id = ?`, lobbyRoomId,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int]int)
	for rows.Next() {
		var slotId, count int
		if err := rows.Scan(&slotId, &count); err != nil {
			return nil, err
		}
		result[slotId] = count
	}
	return result, rows.Err()
}

func (s *RoomStore) FindByLobbyRoomId(lobbyRoomId string) (*RoomRecord, error) {
	var r RoomRecord
	var ts int64
	var perSlotPasswords, deathlinkDisabled, reducedAccess int
	err := s.db.QueryRow(
		`SELECT lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, disabled, per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability FROM rooms WHERE lobby_room_id = ?`,
		lobbyRoomId,
	).Scan(&r.LobbyRoomId, &r.ApRoomId, &r.NormalId, &r.ReducedId, &ts, &r.Disabled, &perSlotPasswords, &deathlinkDisabled, &reducedAccess, &r.DeathlinkProbability)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.CreatedAt = time.Unix(ts, 0)
	r.PerSlotPasswords = perSlotPasswords == 1
	r.DeathlinkDisabled = deathlinkDisabled == 1
	r.ReducedAccess = reducedAccess == 1
	return &r, nil
}

func (s *RoomStore) LoadAll() ([]RoomRecord, error) {
	rows, err := s.db.Query(`SELECT lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability FROM rooms WHERE disabled = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var ts int64
		var perSlotPasswords, deathlinkDisabled, reducedAccess int
		if err := rows.Scan(&r.LobbyRoomId, &r.ApRoomId, &r.NormalId, &r.ReducedId, &ts, &perSlotPasswords, &deathlinkDisabled, &reducedAccess, &r.DeathlinkProbability); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(ts, 0)
		r.PerSlotPasswords = perSlotPasswords == 1
		r.DeathlinkDisabled = deathlinkDisabled == 1
		r.ReducedAccess = reducedAccess == 1
		records = append(records, r)
	}
	return records, rows.Err()
}
