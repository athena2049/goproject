package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof"

	_ "github.com/go-sql-driver/mysql"
)

const (
	defaultDatabase = "app_search"
	defaultTable    = "app_names"
	defaultTotal    = uint64(100_000_000)
	defaultBatch    = 2_000
	defaultKafkaURL = "localhost:9092"
	defaultTopic    = "test"
)

type config struct {
	Mode       string
	Host       string
	Port       string
	User       string
	Password   string
	Database   string
	Table      string
	Total      uint64
	BatchSize  int
	Workers    int
	InsertDate string
	KafkaURL   string
	Topic      string
	KafkaBatch int
	KafkaGroup string
	PprofAddr  string
	PprofHold  bool
}

type appRecord struct {
	Name string
	Date string
}

func main() {
	start := time.Now()
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("failed: %v", err)
	}
	log.Printf("cost: %f\nm", time.Since(start).Minutes())
	holdForPprof(ctx, cfg)
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.Mode, "mode", "seed", "run mode: seed, kafka-producer, or kafka-consumer")
	flag.StringVar(&cfg.Host, "host", getenv("MYSQL_HOST", "127.0.0.1"), "MySQL host")
	flag.StringVar(&cfg.Port, "port", getenv("MYSQL_PORT", "3306"), "MySQL port")
	flag.StringVar(&cfg.User, "user", getenv("MYSQL_USER", "root"), "MySQL user")
	flag.StringVar(&cfg.Password, "password", getenv("MYSQL_PASSWORD", "root123"), "MySQL password")
	flag.StringVar(&cfg.Database, "db", defaultDatabase, "database to create/use")
	flag.StringVar(&cfg.Table, "table", defaultTable, "table to create/use")
	flag.Uint64Var(&cfg.Total, "total", defaultTotal, "number of records to insert")
	flag.IntVar(&cfg.BatchSize, "batch", defaultBatch, "rows per multi-value insert")
	flag.IntVar(&cfg.Workers, "workers", 4, "concurrent insert workers")
	flag.StringVar(&cfg.InsertDate, "insert-date", time.Now().Format(time.DateOnly), "insert date, format YYYY-MM-DD")
	flag.StringVar(&cfg.KafkaURL, "kafka", defaultKafkaURL, "Kafka broker address")
	flag.StringVar(&cfg.Topic, "topic", defaultTopic, "Kafka topic")
	flag.IntVar(&cfg.KafkaBatch, "kafka-batch", 500, "rows/messages per Kafka push")
	flag.StringVar(&cfg.KafkaGroup, "kafka-group", "goproject-consumer", "Kafka consumer group")
	flag.StringVar(&cfg.PprofAddr, "pprof", "127.0.0.1:6060", "pprof listen address, empty to disable")
	flag.BoolVar(&cfg.PprofHold, "pprof-hold", true, "keep process alive after run finishes when pprof is enabled")
	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg config) error {
	startPprofServer(cfg.PprofAddr)

	// go run . -mode kafka-consumer -kafka localhost:9092 -topic test -workers 4
	if cfg.Mode == "kafka-producer" {
		return runKafkaProducer(ctx, cfg)
	}
	if cfg.Mode == "kafka-consumer" {
		return runKafkaConsumer(ctx, cfg)
	}
	if cfg.Mode != "seed" {
		return fmt.Errorf("unsupported mode %q, want seed, kafka-producer, or kafka-consumer", cfg.Mode)
	}
	if cfg.BatchSize <= 0 {
		return errors.New("batch must be greater than 0")
	}
	if cfg.Workers <= 0 {
		return errors.New("workers must be greater than 0")
	}
	if _, err := time.Parse(time.DateOnly, cfg.InsertDate); err != nil {
		return fmt.Errorf("insert-date must be YYYY-MM-DD: %w", err)
	}

	adminDB, err := sql.Open("mysql", adminDSN(cfg))
	if err != nil {
		return err
	}
	defer adminDB.Close()
	adminDB.SetMaxOpenConns(4)

	if err := createDatabase(ctx, adminDB, cfg.Database); err != nil {
		return err
	}

	db, err := sql.Open("mysql", databaseDSN(cfg))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.Workers + 4)
	db.SetMaxIdleConns(cfg.Workers)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := createTable(ctx, db, cfg.Table); err != nil {
		return err
	}

	start := time.Now()
	inserted, err := insertRecords(ctx, db, cfg)
	if err != nil {
		return err
	}

	log.Printf("done: inserted=%d elapsed=%s", inserted, time.Since(start).Round(time.Second))
	return nil
}

func startPprofServer(addr string) {
	if addr == "" {
		return
	}

	go func() {
		log.Printf("pprof listening: http://%s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof stopped: %v", err)
		}
	}()
}

func holdForPprof(ctx context.Context, cfg config) {
	if cfg.PprofAddr == "" || !cfg.PprofHold {
		return
	}

	log.Printf("run finished; keeping process alive for pprof: http://%s/debug/pprof/ (press Ctrl+C to exit)", cfg.PprofAddr)
	<-ctx.Done()
}

func createDatabase(ctx context.Context, db *sql.DB, name string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", quoteIdent(name))
	_, err := db.ExecContext(ctx, query)
	return err
}

func createTable(ctx context.Context, db *sql.DB, table string) error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  appname VARCHAR(255) NOT NULL,
  appname_normalized VARCHAR(255) GENERATED ALWAYS AS (LOWER(TRIM(appname))) STORED,
  insert_date DATE NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'generated',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_appname (appname),
  KEY idx_appname_normalized (appname_normalized),
  KEY idx_insert_date (insert_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`, quoteIdent(table))
	_, err := db.ExecContext(ctx, query)
	return err
}

func insertRecords(ctx context.Context, db *sql.DB, cfg config) (uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	batches := make(chan []appRecord, cfg.Workers*2)
	var inserted atomic.Uint64

	var workers sync.WaitGroup
	errCh := make(chan error, cfg.Workers+1)
	for i := 0; i < cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range batches {
				if err := insertBatch(ctx, db, cfg.Table, batch); err != nil {
					cancel()
					errCh <- err
					return
				}
				total := inserted.Add(uint64(len(batch)))
				if total%1_000_000 == 0 {
					log.Printf("progress: inserted=%d", total)
				}
			}
		}()
	}

	go func() {
		defer close(batches)
		errCh <- produceBatches(ctx, cfg, batches)
	}()

	workers.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return inserted.Load(), err
		}
	}

	return inserted.Load(), nil
}

func produceBatches(ctx context.Context, cfg config, out chan<- []appRecord) error {
	return produceGenerated(ctx, cfg, out)
}

func produceGenerated(ctx context.Context, cfg config, out chan<- []appRecord) error {
	batch := make([]appRecord, 0, cfg.BatchSize)
	for i := uint64(1); i <= cfg.Total; i++ {
		batch = append(batch, appRecord{
			Name: generatedAppName(i),
			Date: cfg.InsertDate,
		})
		if len(batch) == cfg.BatchSize {
			if err := sendBatch(ctx, out, batch); err != nil {
				return err
			}
			batch = make([]appRecord, 0, cfg.BatchSize)
		}
	}
	if len(batch) > 0 {
		return sendBatch(ctx, out, batch)
	}
	return nil
}

func sendBatch(ctx context.Context, out chan<- []appRecord, batch []appRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- batch:
		return nil
	}
}

func insertBatch(ctx context.Context, db *sql.DB, table string, batch []appRecord) error {
	if len(batch) == 0 {
		return nil
	}

	var b strings.Builder
	b.Grow(len(batch) * 16)
	fmt.Fprintf(&b, "INSERT INTO %s (appname, insert_date, source) VALUES ", quoteIdent(table))

	args := make([]any, 0, len(batch)*3)
	for i, record := range batch {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?)")
		args = append(args, record.Name, record.Date, "generated")
	}

	_, err := db.ExecContext(ctx, b.String(), args...)
	return err
}

func generatedAppName(n uint64) string {
	categories := [...]string{"chat", "pay", "map", "music", "video", "photo", "game", "shop", "travel", "health"}
	brands := [...]string{"nova", "pixel", "orbit", "cloud", "swift", "spark", "daily", "bright", "green", "open"}
	return fmt.Sprintf("%s-%s-app-%09d", brands[n%uint64(len(brands))], categories[n%uint64(len(categories))], n)
}

func adminDSN(cfg config) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true&multiStatements=false&charset=utf8mb4,utf8",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)
}

func databaseDSN(cfg config) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=false&charset=utf8mb4,utf8",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
