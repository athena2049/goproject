package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// go run . -mode kafka-consumer -kafka localhost:9092 -topic test -workers 4

type kafkaConsumerEvent struct {
	kind    kafkaConsumerEventKind
	message kafka.Message
	err     error
}

type kafkaConsumerEventKind int

const (
	kafkaConsumerFetched kafkaConsumerEventKind = iota
	kafkaConsumerDone
)

type partitionCommitState struct {
	offsets  []int64
	messages map[int64]kafka.Message
	done     map[int64]bool
}

func runKafkaConsumer(ctx context.Context, cfg config) error {
	if cfg.Workers <= 0 {
		return errors.New("workers must be greater than 0")
	}
	if cfg.KafkaGroup == "" {
		return errors.New("kafka-group must not be empty")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaURL},
		Topic:          cfg.Topic,
		GroupID:        cfg.KafkaGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan kafka.Message, cfg.Workers*2)
	events := make(chan kafkaConsumerEvent, cfg.Workers*4)
	errCh := make(chan error, cfg.Workers+2)

	var processed atomic.Uint64
	var workers sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		workers.Add(1)
		go kafkaConsumerWorker(ctx, i+1, jobs, events, &processed, &workers)
	}

	var committer sync.WaitGroup
	committer.Add(1)
	go kafkaCommitManager(ctx, reader, events, errCh, cancel, &committer)

	log.Printf("kafka consumer started: broker=%s topic=%s group=%s workers=%d", cfg.KafkaURL, cfg.Topic, cfg.KafkaGroup, cfg.Workers)
	readErr := fetchKafkaMessages(ctx, reader, jobs, events)

	close(jobs)
	workers.Wait()
	close(events)
	committer.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return readErr
	}

	log.Printf("kafka consumer stopped: processed=%d", processed.Load())
	return nil
}

func fetchKafkaMessages(ctx context.Context, reader *kafka.Reader, jobs chan<- kafka.Message, events chan<- kafkaConsumerEvent) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		if err := sendKafkaConsumerEvent(ctx, events, kafkaConsumerEvent{kind: kafkaConsumerFetched, message: message}); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- message:
		}
	}
}

func kafkaConsumerWorker(ctx context.Context, id int, jobs <-chan kafka.Message, events chan<- kafkaConsumerEvent, processed *atomic.Uint64, workers *sync.WaitGroup) {
	defer workers.Done()

	for message := range jobs {
		err := processKafkaMessage(ctx, id, message)
		if err == nil {
			total := processed.Add(1)
			if total%1000 == 0 {
				log.Printf("kafka consumer progress: processed=%d", total)
			}
		}

		event := kafkaConsumerEvent{
			kind:    kafkaConsumerDone,
			message: message,
			err:     err,
		}
		if sendKafkaConsumerEvent(ctx, events, event) != nil {
			return
		}
	}
}

func processKafkaMessage(ctx context.Context, workerID int, message kafka.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var payload searchMessage
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		return fmt.Errorf("decode kafka message partition=%d offset=%d: %w", message.Partition, message.Offset, err)
	}

	log.Printf("kafka worker=%d partition=%d offset=%d key=%s keyword=%s uid=%d scene=%s",
		workerID, message.Partition, message.Offset, string(message.Key), payload.Keyword, payload.UID, payload.Scene)
	return nil
}

func kafkaCommitManager(ctx context.Context, reader *kafka.Reader, events <-chan kafkaConsumerEvent, errCh chan<- error, cancel context.CancelFunc, committer *sync.WaitGroup) {
	defer committer.Done()

	states := make(map[int]*partitionCommitState)
	for event := range events {
		if event.err != nil {
			cancel()
			errCh <- event.err
			continue
		}

		state := kafkaPartitionState(states, event.message)
		switch event.kind {
		case kafkaConsumerFetched:
			state.messages[event.message.Offset] = event.message
			state.offsets = append(state.offsets, event.message.Offset)
		case kafkaConsumerDone:
			state.done[event.message.Offset] = true
			if err := commitReadyKafkaOffsets(ctx, reader, state); err != nil {
				cancel()
				errCh <- err
			}
		}
	}

	for _, state := range states {
		if err := commitReadyKafkaOffsets(ctx, reader, state); err != nil {
			errCh <- err
		}
	}
}

func kafkaPartitionState(states map[int]*partitionCommitState, message kafka.Message) *partitionCommitState {
	state, ok := states[message.Partition]
	if !ok {
		state = &partitionCommitState{
			messages: make(map[int64]kafka.Message),
			done:     make(map[int64]bool),
		}
		states[message.Partition] = state
	}
	return state
}

func commitReadyKafkaOffsets(ctx context.Context, reader *kafka.Reader, state *partitionCommitState) error {
	var lastReady kafka.Message
	hasReady := false

	for len(state.offsets) > 0 && state.done[state.offsets[0]] {
		offset := state.offsets[0]
		message, ok := state.messages[offset]
		if !ok {
			break
		}
		lastReady = message
		hasReady = true
		delete(state.done, offset)
		delete(state.messages, offset)
		state.offsets = state.offsets[1:]
	}
	if !hasReady {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := reader.CommitMessages(commitCtx, lastReady); err != nil {
		return fmt.Errorf("commit kafka offset partition=%d offset=%d: %w", lastReady.Partition, lastReady.Offset, err)
	}
	return nil
}

func sendKafkaConsumerEvent(ctx context.Context, events chan<- kafkaConsumerEvent, event kafkaConsumerEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case events <- event:
		return nil
	}
}
