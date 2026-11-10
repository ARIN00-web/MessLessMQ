// Package delivery makes the broker's delivery guarantees explicit at the
// consumer-processing boundary. It deliberately does not claim atomicity with
// arbitrary external systems.
package delivery

import (
	"context"
	"errors"
	"fmt"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/group"
)

type Mode uint8

const (
	AtMostOnce Mode = iota + 1
	AtLeastOnce
)

var (
	ErrInvalidMode      = errors.New("invalid delivery mode")
	ErrNilCoordinator   = errors.New("delivery processor requires a group coordinator")
	ErrNilHandler       = errors.New("delivery processor requires a record handler")
	ErrProcessingFailed = errors.New("record processing failed")
)

// Handler processes one record. A nil error means the application considers
// the record successfully processed.
type Handler func(context.Context, consumer.Record) error

// BatchResult summarizes fetching, processing and durable acknowledgement.
type BatchResult struct {
	Fetched             int
	Processed           int
	NextOffset          uint64
	CommittedNextOffset uint64
}

// ProcessingError identifies the record whose processing failed and whether
// its batch had already been committed.
type ProcessingError struct {
	Topic       string
	PartitionID uint32
	Offset      uint64
	Committed   bool
	Cause       error
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf(
		"%v: topic=%q partition=%d offset=%d committed=%t: %v",
		ErrProcessingFailed,
		e.Topic,
		e.PartitionID,
		e.Offset,
		e.Committed,
		e.Cause,
	)
}

func (e *ProcessingError) Unwrap() error { return e.Cause }

// Processor applies at-most-once or at-least-once processing rules to a
// consumer group member's fetched records.
type Processor struct {
	coordinator *group.Coordinator
	mode        Mode
	handler     Handler
}

func NewProcessor(coordinator *group.Coordinator, mode Mode, handler Handler) (*Processor, error) {
	if coordinator == nil {
		return nil, ErrNilCoordinator
	}
	if mode != AtMostOnce && mode != AtLeastOnce {
		return nil, ErrInvalidMode
	}
	if handler == nil {
		return nil, ErrNilHandler
	}
	return &Processor{coordinator: coordinator, mode: mode, handler: handler}, nil
}

// ProcessBatch fetches a bounded batch and applies the selected delivery
// guarantee. AtMostOnce commits the whole batch before invoking the handler.
// AtLeastOnce processes and commits one record at a time; an uncommitted tail
// is rewound in memory so it can be fetched again.
func (p *Processor) ProcessBatch(
	ctx context.Context,
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	maxMessages uint32,
) (BatchResult, error) {
	if p == nil || p.coordinator == nil {
		return BatchResult{}, ErrNilCoordinator
	}
	if ctx == nil {
		ctx = context.Background()
	}

	batch, err := p.coordinator.Fetch(
		groupID,
		memberID,
		generation,
		topicName,
		partitionID,
		maxMessages,
	)
	if err != nil {
		return BatchResult{}, err
	}

	result := BatchResult{
		Fetched:    len(batch.Records),
		NextOffset: batch.NextOffset,
	}
	if len(batch.Records) == 0 {
		return result, nil
	}

	firstOffset := batch.Records[0].Offset
	if p.mode == AtMostOnce {
		// Commit before processing. A crash after this point can lose work,
		// but a record will not be redelivered by the group.
		if err := p.coordinator.CommitOffset(
			groupID, memberID, generation,
			topicName, partitionID, batch.NextOffset,
		); err != nil {
			rewindErr := p.coordinator.SetReadPosition(
				groupID, memberID, generation,
				topicName, partitionID, firstOffset,
			)
			return result, errors.Join(
				fmt.Errorf("commit at-most-once batch before processing: %w", err),
				wrapRewindError(rewindErr),
			)
		}
		result.CommittedNextOffset = batch.NextOffset

		for _, record := range batch.Records {
			if err := ctx.Err(); err != nil {
				return result, &ProcessingError{
					Topic: topicName, PartitionID: partitionID,
					Offset: record.Offset, Committed: true, Cause: err,
				}
			}
			if err := p.handler(ctx, record); err != nil {
				return result, &ProcessingError{
					Topic: topicName, PartitionID: partitionID,
					Offset: record.Offset, Committed: true, Cause: err,
				}
			}
			result.Processed++
		}
		return result, nil
	}

	// At-least-once: persist progress only after each record's handler returns
	// success. If processing or committing fails, rewind to the last durable
	// commit; work after that point may be delivered again.
	lastCommitted := firstOffset
	for _, record := range batch.Records {
		if err := ctx.Err(); err != nil {
			rewindErr := p.coordinator.SetReadPosition(
				groupID, memberID, generation,
				topicName, partitionID, lastCommitted,
			)
			return result, errors.Join(&ProcessingError{
				Topic: topicName, PartitionID: partitionID,
				Offset: record.Offset, Committed: false, Cause: err,
			}, wrapRewindError(rewindErr))
		}

		if err := p.handler(ctx, record); err != nil {
			rewindErr := p.coordinator.SetReadPosition(
				groupID, memberID, generation,
				topicName, partitionID, lastCommitted,
			)
			return result, errors.Join(&ProcessingError{
				Topic: topicName, PartitionID: partitionID,
				Offset: record.Offset, Committed: false, Cause: err,
			}, wrapRewindError(rewindErr))
		}

		next := record.Offset + 1
		if err := p.coordinator.CommitOffset(
			groupID, memberID, generation,
			topicName, partitionID, next,
		); err != nil {
			rewindErr := p.coordinator.SetReadPosition(
				groupID, memberID, generation,
				topicName, partitionID, lastCommitted,
			)
			return result, errors.Join(
				fmt.Errorf("commit at-least-once offset %d: %w", next, err),
				wrapRewindError(rewindErr),
			)
		}

		lastCommitted = next
		result.CommittedNextOffset = next
		result.Processed++
	}

	return result, nil
}

func wrapRewindError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("rewind consumer position: %w", err)
}

// EffectHandler computes durable effect intents for one input record. It
// should be deterministic and must not perform non-idempotent external side
// effects before returning. Actual external delivery of the stored intents
// must use its own idempotency mechanism.
type EffectHandler func(context.Context, consumer.Record) ([][]byte, error)

// ProcessEffectivelyOnce records each input's effect intents in EffectStore
// before committing the input offset. If the process crashes after recording
// effects but before committing the offset, replay finds the existing event
// key and does not create the effects again.
//
// This gives effectively-once creation of local durable effect intents. It
// does not provide a distributed transaction with an arbitrary external DB,
// payment API, email service, or other side-effecting system.
func (p *Processor) ProcessEffectivelyOnce(
	ctx context.Context,
	store *EffectStore,
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	maxMessages uint32,
	handler EffectHandler,
) (BatchResult, error) {
	if p == nil || p.coordinator == nil {
		return BatchResult{}, ErrNilCoordinator
	}
	if store == nil {
		return BatchResult{}, ErrEffectStoreClosed
	}
	if handler == nil {
		return BatchResult{}, ErrNilHandler
	}
	if ctx == nil {
		ctx = context.Background()
	}

	batch, err := p.coordinator.Fetch(
		groupID, memberID, generation, topicName, partitionID, maxMessages,
	)
	if err != nil {
		return BatchResult{}, err
	}
	result := BatchResult{Fetched: len(batch.Records), NextOffset: batch.NextOffset}
	if len(batch.Records) == 0 {
		return result, nil
	}

	firstOffset := batch.Records[0].Offset
	for _, record := range batch.Records {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(err, p.rewind(groupID, memberID, generation, topicName, partitionID, firstOffset))
		}
		eventID := EventID{
			GroupID: groupID, Topic: topicName,
			PartitionID: partitionID, Offset: record.Offset,
		}
		if _, exists, err := store.Get(eventID); err != nil {
			return result, errors.Join(err, p.rewind(groupID, memberID, generation, topicName, partitionID, firstOffset))
		} else if exists {
			// The effect was durably recorded during an earlier attempt. Skip
			// computation and continue toward committing the input offset.
			continue
		}

		effects, err := handler(ctx, record)
		if err != nil {
			return result, errors.Join(
				&ProcessingError{Topic: topicName, PartitionID: partitionID, Offset: record.Offset, Cause: err},
				p.rewind(groupID, memberID, generation, topicName, partitionID, firstOffset),
			)
		}
		if _, err := store.ApplyOnce(eventID, effects); err != nil {
			return result, errors.Join(err, p.rewind(groupID, memberID, generation, topicName, partitionID, firstOffset))
		}
		result.Processed++
	}

	if err := p.coordinator.CommitOffset(
		groupID, memberID, generation, topicName, partitionID, batch.NextOffset,
	); err != nil {
		return result, errors.Join(
			fmt.Errorf("commit offset after durable effects: %w", err),
			p.rewind(groupID, memberID, generation, topicName, partitionID, firstOffset),
		)
	}
	result.CommittedNextOffset = batch.NextOffset
	return result, nil
}

func (p *Processor) rewind(
	groupID, memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	offset uint64,
) error {
	if err := p.coordinator.SetReadPosition(
		groupID, memberID, generation, topicName, partitionID, offset,
	); err != nil {
		return fmt.Errorf("rewind consumer after incomplete batch: %w", err)
	}
	return nil
}
