package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type LogSource string

const (
	LogSourceClient LogSource = "client"
	LogSourceApx    LogSource = "apx"
	LogSourceServer LogSource = "server"
)

var linePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

type lokiEntry struct {
	slot      string
	source    LogSource
	line      []byte
	timestamp time.Time
}

type LokiLogger struct {
	endpoint      string
	roomId        string
	ch            chan lokiEntry
	batchSize     int
	flushInterval time.Duration
	client        *http.Client
	wg            sync.WaitGroup
}

func NewLokiLogger(endpoint, roomId string) *LokiLogger {
	l := &LokiLogger{
		endpoint:      endpoint,
		roomId:        roomId,
		ch:            make(chan lokiEntry, 16384),
		batchSize:     500,
		flushInterval: 500 * time.Millisecond,
		client:        &http.Client{Timeout: 5 * time.Second},
	}
	l.wg.Add(1)
	go l.run()
	return l
}

func (l *LokiLogger) Log(slot string, source LogSource, msg []byte) {
	buf := linePool.Get().(*[]byte)
	*buf = append((*buf)[:0], msg...)

	select {
	case l.ch <- lokiEntry{
		slot:      slot,
		source:    source,
		line:      *buf,
		timestamp: time.Now(),
	}:
	default:
		linePool.Put(buf) // channel full, return immediately
	}
}

func (l *LokiLogger) Close() {
	close(l.ch)
	l.wg.Wait()
}

type streamKey struct {
	slot      string
	cmd       string
	direction LogSource
}

type lokiStream struct {
	key     streamKey
	entries []lokiEntry
}

func (l *LokiLogger) run() {
	defer l.wg.Done()

	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()

	batch := make(map[streamKey][]lokiEntry)
	count := 0

	flush := func() {
		if count == 0 {
			return
		}
		streams := make([]lokiStream, 0, len(batch))
		for key, entries := range batch {
			streams = append(streams, lokiStream{key: key, entries: entries})
		}
		if err := l.push(streams); err != nil {
			log.Printf("loki push error: %v", err)
		}
		// return buffers to pool after push completes
		for _, s := range streams {
			for _, e := range s.entries {
				b := e.line
				linePool.Put(&b)
			}
		}
		batch = make(map[streamKey][]lokiEntry)
		count = 0
	}

	for {
		select {
		case entry, ok := <-l.ch:
			if !ok {
				flush()
				return
			}
			key := streamKey{
				slot:      entry.slot,
				direction: entry.source,
			}
			batch[key] = append(batch[key], entry)
			count++
			if count >= l.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

type lokiPushPayload struct {
	Streams []lokiStreamPayload `json:"streams"`
}

type lokiStreamPayload struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

func (l *LokiLogger) push(streams []lokiStream) error {
	payload := lokiPushPayload{
		Streams: make([]lokiStreamPayload, 0, len(streams)),
	}

	for _, s := range streams {
		labels := map[string]string{
			"room_id":   l.roomId,
			"slot":      s.key.slot,
			"cmd":       s.key.cmd,
			"direction": string(s.key.direction),
		}

		values := make([][2]string, len(s.entries))
		for i, e := range s.entries {
			values[i] = [2]string{
				strconv.FormatInt(e.timestamp.UnixNano(), 10),
				string(e.line), // copy into string for JSON serialisation
			}
		}

		payload.Streams = append(payload.Streams, lokiStreamPayload{
			Stream: labels,
			Values: values,
		})
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, l.endpoint+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loki returned status %d", resp.StatusCode)
	}
	return nil
}
