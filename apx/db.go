package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/klauspost/compress/zstd"
)

type RoomStore struct {
	pool    *pgxpool.Pool
	enabled bool
}

type RoomRecord struct {
	CreatedAt            time.Time
	Definition           *RoomDefinition
	LobbyRoomId          string
	NormalId             int
	ReducedId            int
	DeathlinkProbability float64
	Disabled             bool
	PerSlotPasswords     bool
	DeathlinkDisabled    bool
	ReducedAccess        bool
}

func NewRoomStore(connString string) (*RoomStore, error) {
	if connString == "" {
		log.Printf("Room record saving disabled, postgres not configured")
		return &RoomStore{enabled: false}, nil
	}

	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("opening postgres: %w", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	// migrations use database/sql; adapt the pool rather than opening a second connection
	db := stdlib.OpenDBFromPool(pool)
	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return &RoomStore{pool: pool, enabled: true}, nil
}

func (s *RoomStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *RoomStore) Disable(lobbyRoomId string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `UPDATE rooms SET disabled = TRUE WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) Enable(lobbyRoomId string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`UPDATE rooms SET disabled = FALSE WHERE lobby_room_id = $1`, lobbyRoomId,
	)
	return err
}

func (s *RoomStore) Save(r RoomRecord) error {
	if !s.enabled {
		return nil
	}
	defBytes, err := marshalDefinition(r.Definition)
	if err != nil {
		return fmt.Errorf("marshalling room definition: %w", err)
	}
	_, err = s.pool.Exec(context.Background(), `
			INSERT INTO rooms (lobby_room_id, normal_port, reduced_port, created_at, per_slot_passwords,
												 deathlink_disabled, reduced_access, definition)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (lobby_room_id) DO UPDATE SET
					normal_port        = excluded.normal_port,
					reduced_port       = excluded.reduced_port,
					per_slot_passwords = excluded.per_slot_passwords,
					deathlink_disabled = excluded.deathlink_disabled,
					reduced_access     = excluded.reduced_access,
					definition         = excluded.definition
	`, r.LobbyRoomId, r.NormalId, r.ReducedId, r.CreatedAt.Unix(),
		r.PerSlotPasswords, r.DeathlinkDisabled, r.ReducedAccess, defBytes)
	return err
}

func (s *RoomStore) Delete(lobbyRoomId string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM rooms WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) SaveDeathlinkProbability(lobbyRoomId string, prob float64) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`UPDATE rooms SET deathlink_probability = $1 WHERE lobby_room_id = $2`,
		prob, lobbyRoomId,
	)
	return err
}

func (s *RoomStore) SetFullFeedSlot(lobbyRoomId string, slotId int) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`INSERT INTO room_full_feed (lobby_room_id, slot_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) DeleteFullFeedSlot(lobbyRoomId string, slotId int) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`DELETE FROM room_full_feed WHERE lobby_room_id = $1 AND slot_id = $2`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) LoadFullFeedSlots(lobbyRoomId string) ([]int, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT slot_id FROM room_full_feed WHERE lobby_room_id = $1`, lobbyRoomId,
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
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM room_full_feed WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) AddSlotBounceExclusion(lobbyRoomId string, slotId int) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`INSERT INTO room_slot_bounce_exclusions (lobby_room_id, slot_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) DeleteSlotBounceExclusion(lobbyRoomId string, slotId int) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`DELETE FROM room_slot_bounce_exclusions WHERE lobby_room_id = $1 AND slot_id = $2`,
		lobbyRoomId, slotId,
	)
	return err
}

func (s *RoomStore) LoadSlotBounceExclusions(lobbyRoomId string) ([]int, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT slot_id FROM room_slot_bounce_exclusions WHERE lobby_room_id = $1`, lobbyRoomId,
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
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM room_slot_bounce_exclusions WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) AddBounceTagExclusion(lobbyRoomId string, slotId int, tag string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`INSERT INTO room_bounce_tag_exclusions (lobby_room_id, slot_id, tag) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		lobbyRoomId, slotId, tag,
	)
	return err
}

func (s *RoomStore) DeleteBounceTagExclusion(lobbyRoomId string, slotId int, tag string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = $1 AND slot_id = $2 AND tag = $3`,
		lobbyRoomId, slotId, tag,
	)
	return err
}

func (s *RoomStore) LoadBounceTagExclusions(lobbyRoomId string) (map[int][]string, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT slot_id, tag FROM room_bounce_tag_exclusions WHERE lobby_room_id = $1`, lobbyRoomId,
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
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) DeleteBounceTagExclusionsForTag(lobbyRoomId string, tag string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`DELETE FROM room_bounce_tag_exclusions WHERE lobby_room_id = $1 AND tag = $2`,
		lobbyRoomId, tag,
	)
	return err
}

func (s *RoomStore) AddAltConnectName(lobbyRoomId, altName, slotName string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`INSERT INTO room_alt_connect_names (lobby_room_id, alt_name, slot_name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		lobbyRoomId, altName, slotName,
	)
	return err
}

func (s *RoomStore) DeleteAltConnectName(lobbyRoomId, altName string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(),
		`DELETE FROM room_alt_connect_names WHERE lobby_room_id = $1 AND alt_name = $2`,
		lobbyRoomId, altName,
	)
	return err
}

func (s *RoomStore) LoadAltConnectNamesBySlot(lobbyRoomId, slotName string) ([]string, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT alt_name FROM room_alt_connect_names WHERE lobby_room_id = $1 AND slot_name = $2`,
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
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT alt_name, slot_name FROM room_alt_connect_names WHERE lobby_room_id = $1`, lobbyRoomId,
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
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM room_alt_connect_names WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) IncrementSlotDeathCount(lobbyRoomId string, slotId int) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `
        INSERT INTO room_slot_deaths (lobby_room_id, slot_id, count)
        VALUES ($1, $2, 1)
        ON CONFLICT (lobby_room_id, slot_id) DO UPDATE SET count = room_slot_deaths.count + 1
    `, lobbyRoomId, slotId)
	return err
}

func (s *RoomStore) LoadSlotDeaths(lobbyRoomId string) (map[int]int, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT slot_id, count FROM room_slot_deaths WHERE lobby_room_id = $1`, lobbyRoomId,
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

func (s *RoomStore) DeleteSlotDeaths(lobbyRoomId string) error {
	if !s.enabled {
		return nil
	}
	_, err := s.pool.Exec(context.Background(), `DELETE FROM room_slot_deaths WHERE lobby_room_id = $1`, lobbyRoomId)
	return err
}

func (s *RoomStore) FindByLobbyRoomId(lobbyRoomId string) (*RoomRecord, error) {
	if !s.enabled {
		return nil, nil
	}
	var r RoomRecord
	var ts int64
	err := s.pool.QueryRow(context.Background(), `
        SELECT lobby_room_id, normal_port, reduced_port, created_at, disabled,
               per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability
        FROM rooms WHERE lobby_room_id = $1`,
		lobbyRoomId,
	).Scan(&r.LobbyRoomId, &r.NormalId, &r.ReducedId, &ts, &r.Disabled,
		&r.PerSlotPasswords, &r.DeathlinkDisabled, &r.ReducedAccess, &r.DeathlinkProbability)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(ts, 0)
	return &r, nil
}

func (s *RoomStore) FindFullByLobbyRoomId(lobbyRoomId string) (*RoomRecord, error) {
	if !s.enabled {
		return nil, nil
	}
	var r RoomRecord
	var ts int64
	var defBytes []byte
	err := s.pool.QueryRow(context.Background(), `
			SELECT lobby_room_id, normal_port, reduced_port, created_at, disabled,
						 per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability, definition
			FROM rooms WHERE lobby_room_id = $1`,
		lobbyRoomId,
	).Scan(&r.LobbyRoomId, &r.NormalId, &r.ReducedId, &ts, &r.Disabled,
		&r.PerSlotPasswords, &r.DeathlinkDisabled, &r.ReducedAccess, &r.DeathlinkProbability, &defBytes)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	if len(defBytes) > 0 {
		def, err := unmarshalDefinition(defBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling room definition: %w", err)
		}
		r.Definition = def
	}
	r.CreatedAt = time.Unix(ts, 0)
	return &r, nil
}

func (s *RoomStore) LoadAllSlim() ([]RoomRecord, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(), `
			SELECT lobby_room_id, normal_port, reduced_port, created_at, disabled,
						 per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability
			FROM rooms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var ts int64
		if err := rows.Scan(&r.LobbyRoomId, &r.NormalId, &r.ReducedId, &ts, &r.Disabled,
			&r.PerSlotPasswords, &r.DeathlinkDisabled, &r.ReducedAccess, &r.DeathlinkProbability); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(ts, 0)
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *RoomStore) CountRooms() (total int, err error) {
	if !s.enabled {
		return 0, nil
	}
	err = s.pool.QueryRow(context.Background(), `
			SELECT
					COUNT(*)
			FROM rooms
	`).Scan(&total)
	return total, err
}

func (s *RoomStore) LoadAll() ([]RoomRecord, error) {
	if !s.enabled {
		return nil, nil
	}
	rows, err := s.pool.Query(context.Background(), `
			SELECT lobby_room_id, normal_port, reduced_port, created_at,
						 per_slot_passwords, deathlink_disabled, reduced_access, deathlink_probability, definition
			FROM rooms WHERE disabled = FALSE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var ts int64
		var defBytes []byte
		if err := rows.Scan(&r.LobbyRoomId, &r.NormalId, &r.ReducedId, &ts,
			&r.PerSlotPasswords, &r.DeathlinkDisabled, &r.ReducedAccess, &r.DeathlinkProbability, &defBytes); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(ts, 0)
		if len(defBytes) > 0 {
			def, err := unmarshalDefinition(defBytes)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling definition for room %s: %w", r.LobbyRoomId, err)
			}
			r.Definition = def
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *RoomStore) AddDataPackageRefs(lobbyRoomId string, checksums []string) error {
	if !s.enabled {
		return nil
	}
	for _, checksum := range checksums {
		_, err := s.pool.Exec(context.Background(), `
					INSERT INTO room_datapackage_refs (lobby_room_id, checksum)
					VALUES ($1, $2)
					ON CONFLICT DO NOTHING
			`, lobbyRoomId, checksum)
		if err != nil {
			return fmt.Errorf("adding datapackage ref %q for room %q: %w", checksum, lobbyRoomId, err)
		}
	}
	return nil
}

// RemoveDataPackageRefs removes refs for the room and returns checksums
// no longer referenced by any room (safe to delete from disk).
func (s *RoomStore) RemoveDataPackageRefs(lobbyRoomId string, checksums []string) ([]string, error) {
	if !s.enabled {
		return nil, nil
	}
	_, err := s.pool.Exec(context.Background(), `
			DELETE FROM room_datapackage_refs
			WHERE lobby_room_id = $1 AND checksum = ANY($2)
	`, lobbyRoomId, checksums)
	if err != nil {
		return nil, fmt.Errorf("removing datapackage refs for room %q: %w", lobbyRoomId, err)
	}

	// Find which of the deleted checksums are now unreferenced
	rows, err := s.pool.Query(context.Background(), `
			SELECT unnest($1::text[]) AS checksum
			EXCEPT
			SELECT checksum FROM room_datapackage_refs
	`, checksums)
	if err != nil {
		return nil, fmt.Errorf("finding orphaned datapackages: %w", err)
	}
	defer rows.Close()

	var orphaned []string
	for rows.Next() {
		var checksum string
		if err := rows.Scan(&checksum); err != nil {
			return nil, err
		}
		orphaned = append(orphaned, checksum)
	}
	return orphaned, rows.Err()
}

func marshalDefinition(def *RoomDefinition) ([]byte, error) {
	raw, err := json.Marshal(def)
	if err != nil {
		return nil, err
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	return enc.EncodeAll(raw, nil), nil
}

func unmarshalDefinition(data []byte) (*RoomDefinition, error) {
	dec, _ := zstd.NewReader(nil)
	raw, err := dec.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	var def RoomDefinition
	return &def, json.Unmarshal(raw, &def)
}
