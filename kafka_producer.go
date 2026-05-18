package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// go run . -mode kafka-producer -kafka-batch 500
type appNameRow struct {
	ID      uint64
	AppName string
}

type searchMessage struct {
	Keyword   string `json:"keyword"`
	Timestamp int64  `json:"timestamp"`
	UID       int64  `json:"uid"`
	Scene     string `json:"scene"`
}

var jsonBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func runKafkaProducer(ctx context.Context, cfg config) error {
	if cfg.KafkaBatch <= 0 {
		return errors.New("kafka-batch must be greater than 0")
	}
	db, err := sql.Open("mysql", databaseDSN(cfg))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaURL),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		BatchSize:    cfg.KafkaBatch,
		BatchTimeout: 5 * time.Millisecond,
	}
	defer writer.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	var lastID uint64
	var pushed uint64

	for {
		rows, err := fetchAppNames(ctx, db, cfg.Table, lastID, cfg.KafkaBatch)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		if err = processKafkaRows(ctx, writer, rows, random); err != nil {
			cancel()
			return err
		}

		pushed += uint64(len(rows))
		lastID = rows[len(rows)-1].ID
		log.Printf("kafka progress: pushed=%d last_id=%d", pushed, lastID)
	}

	log.Printf("kafka done: pushed=%d topic=%s broker=%s", pushed, cfg.Topic, cfg.KafkaURL)
	return nil
}

func processKafkaRows(ctx context.Context, writer *kafka.Writer, rows []appNameRow, random *rand.Rand) error {
	messages, err := buildKafkaMessages(rows, random)
	if err != nil {
		return fmt.Errorf("build kafka msg err=%w", err)
	}
	if err = writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write message err=%w", err)
	}
	return nil
}

func fetchAppNames(ctx context.Context, db *sql.DB, table string, afterID uint64, limit int) ([]appNameRow, error) {
	query := fmt.Sprintf("SELECT id, appname FROM %s WHERE id > ? ORDER BY id ASC LIMIT ?", quoteIdent(table))
	rows, err := db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]appNameRow, 0, limit)
	for rows.Next() {
		var row appNameRow
		if err := rows.Scan(&row.ID, &row.AppName); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func buildKafkaMessages(rows []appNameRow, random *rand.Rand) ([]kafka.Message, error) {
	now := time.Now().Unix()
	messages := make([]kafka.Message, 0, len(rows))

	for _, row := range rows {
		value, err := marshalSearchMessage(searchMessage{
			Keyword:   row.AppName,
			Timestamp: now,
			UID:       random.Int63n(900_000_000) + 100_000,
			Scene:     "search",
		})
		if err != nil {
			return nil, err
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", row.ID)),
			Value: value,
			Time:  time.Now(),
		})
	}

	return messages, nil
}

func marshalSearchMessage(message searchMessage) ([]byte, error) {
	buf := jsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufferPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(message); err != nil {
		return nil, err
	}

	value := bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
	return append([]byte(nil), value...), nil
}
