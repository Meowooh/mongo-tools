// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/mongodb/mongo-tools/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// The default value of maxMessageSizeBytes
	// See: https://docs.mongodb.com/manual/reference/command/hello/#mongodb-data-hello.maxMessageSizeBytes
	MAX_MESSAGE_SIZE_BYTES = 48000000

	maxRetryableBulkDocuments = 1000
	generatedObjectIDOverhead = 17
)

type bulkWriteFunc func(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)

type bufferedBulkRetryPolicy struct {
	ctx                   context.Context
	timeout               time.Duration
	shouldRetry           func(error) bool
	shouldRetryWriteError func(mongo.WriteError) bool
	retryDelay            func(int) time.Duration
}

// BufferedBulkInserter implements a bufio.Writer-like design for queuing up
// documents and inserting them in bulk when the given doc limit (or max
// message size) is reached. Must be flushed at the end to ensure that all
// documents are written.
type BufferedBulkInserter struct {
	collection    *mongo.Collection
	writeModels   []mongo.WriteModel
	docLimit      int
	docCount      int
	byteCount     int
	byteLimit     int
	bulkWriteOpts *options.BulkWriteOptions
	bulkWrite     bulkWriteFunc
	retryPolicy   *bufferedBulkRetryPolicy
	upsert        bool
}

func newBufferedBulkInserter(collection *mongo.Collection, docLimit int, ordered bool) *BufferedBulkInserter {
	bb := &BufferedBulkInserter{
		collection:    collection,
		bulkWriteOpts: options.BulkWrite().SetOrdered(ordered),
		bulkWrite:     collection.BulkWrite,
		docLimit:      docLimit,
		// We set the byte limit to be slightly lower than maxMessageSizeBytes so it can fit in one OP_MSG.
		// This may not always be perfect, e.g. we don't count update selectors in byte totals, but it should
		// be good enough to keep memory consumption in check.
		byteLimit:   MAX_MESSAGE_SIZE_BYTES - 100,
		writeModels: make([]mongo.WriteModel, 0, docLimit),
	}
	return bb
}

// NewOrderedBufferedBulkInserter returns an initialized BufferedBulkInserter for performing ordered bulk writes.
func NewOrderedBufferedBulkInserter(collection *mongo.Collection, docLimit int) *BufferedBulkInserter {
	return newBufferedBulkInserter(collection, docLimit, true)
}

// NewOrderedBufferedBulkInserter returns an initialized BufferedBulkInserter for performing unordered bulk writes.
func NewUnorderedBufferedBulkInserter(collection *mongo.Collection, docLimit int) *BufferedBulkInserter {
	return newBufferedBulkInserter(collection, docLimit, false)
}

func (bb *BufferedBulkInserter) SetOrdered(ordered bool) *BufferedBulkInserter {
	bb.bulkWriteOpts.SetOrdered(ordered)
	return bb
}

func (bb *BufferedBulkInserter) SetBypassDocumentValidation(bypass bool) *BufferedBulkInserter {
	bb.bulkWriteOpts.SetBypassDocumentValidation(bypass)
	return bb
}

func (bb *BufferedBulkInserter) SetUpsert(upsert bool) *BufferedBulkInserter {
	bb.upsert = upsert
	return bb
}

// SetRetryPolicy enables retries for errors accepted by shouldRetry. It must be called before inserting documents.
func (bb *BufferedBulkInserter) SetRetryPolicy(
	ctx context.Context,
	timeout time.Duration,
	shouldRetry func(error) bool,
) *BufferedBulkInserter {
	if ctx == nil {
		ctx = context.Background()
	}
	bb.retryPolicy = &bufferedBulkRetryPolicy{
		ctx:         ctx,
		timeout:     timeout,
		shouldRetry: shouldRetry,
		retryDelay:  defaultBulkWriteRetryDelay,
	}

	// The v1 driver adds up to 17 bytes for a missing _id before splitting at
	// MaxBSONSize. Reserve that overhead for every document in a retryable batch.
	bb.byteLimit = MaxBSONSize - maxRetryableBulkDocuments*generatedObjectIDOverhead
	if bb.docLimit > maxRetryableBulkDocuments {
		bb.docLimit = maxRetryableBulkDocuments
	}
	return bb
}

// SetRetryableWriteErrorPolicy enables selective retries for per-model write errors accepted by
// shouldRetryWriteError. Successful models are removed from subsequent attempts. It must be called
// after SetRetryPolicy and before inserting documents.
func (bb *BufferedBulkInserter) SetRetryableWriteErrorPolicy(
	shouldRetryWriteError func(mongo.WriteError) bool,
) *BufferedBulkInserter {
	if bb.retryPolicy != nil {
		bb.retryPolicy.shouldRetryWriteError = shouldRetryWriteError
	}
	return bb
}

// throw away the old bulk and init a new one
func (bb *BufferedBulkInserter) resetBulk() {
	bb.writeModels = bb.writeModels[:0]
	bb.docCount = 0
	bb.byteCount = 0
}

// Insert adds a document to the buffer for bulk insertion. If the buffer becomes full, the bulk write is performed, returning
// any error that occurs.
func (bb *BufferedBulkInserter) Insert(doc interface{}) (*mongo.BulkWriteResult, error) {
	rawBytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("bson encoding error: %v", err)
	}

	return bb.InsertRaw(rawBytes)
}

// Update adds a document to the buffer for bulk update. If the buffer becomes full, the bulk write is performed, returning
// any error that occurs.
func (bb *BufferedBulkInserter) Update(selector, update bson.D) (*mongo.BulkWriteResult, error) {
	rawBytes, err := bson.Marshal(update)
	if err != nil {
		return nil, err
	}

	return bb.addModel(len(rawBytes), mongo.NewUpdateOneModel().SetFilter(selector).SetUpdate(rawBytes).SetUpsert(bb.upsert))
}

// Replace adds a document to the buffer for bulk replacement. If the buffer becomes full, the bulk write is performed, returning
// any error that occurs.
func (bb *BufferedBulkInserter) Replace(selector, replacement bson.D) (*mongo.BulkWriteResult, error) {
	rawBytes, err := bson.Marshal(replacement)
	if err != nil {
		return nil, err
	}

	return bb.addModel(len(rawBytes), mongo.NewReplaceOneModel().SetFilter(selector).SetReplacement(rawBytes).SetUpsert(bb.upsert))
}

// InsertRaw adds a document, represented as raw bson bytes, to the buffer for bulk insertion. If the buffer becomes full,
// the bulk write is performed, returning any error that occurs.
func (bb *BufferedBulkInserter) InsertRaw(rawBytes []byte) (*mongo.BulkWriteResult, error) {
	return bb.addModel(len(rawBytes), mongo.NewInsertOneModel().SetDocument(rawBytes))
}

// Delete adds a document to the buffer for bulk removal. If the buffer becomes full, the bulk delete is performed, returning
// any error that occurs.
func (bb *BufferedBulkInserter) Delete(selector, replacement bson.D) (*mongo.BulkWriteResult, error) {
	return bb.addModel(0, mongo.NewDeleteOneModel().SetFilter(selector))
}

// addModel adds a WriteModel to the buffer. If the buffer becomes full, the bulk write is performed, returning any error
// that occurs.
func (bb *BufferedBulkInserter) addModel(modelSize int, model mongo.WriteModel) (*mongo.BulkWriteResult, error) {
	var result *mongo.BulkWriteResult
	var err error

	if bb.retryPolicy != nil && bb.docCount > 0 && bb.byteCount+modelSize > bb.byteLimit {
		result, err = bb.Flush()
	}

	// Always buffer the current model, even if the pre-flush failed: the
	// caller may treat the flush error as ignorable (e.g. duplicate keys)
	// and keep going, and this document must not be silently dropped.
	bb.docCount++
	bb.byteCount += modelSize
	bb.writeModels = append(bb.writeModels, model)

	if err != nil {
		return result, err
	}

	if bb.docCount >= bb.docLimit || (bb.retryPolicy == nil && bb.byteCount >= bb.byteLimit) {
		return bb.Flush()
	}

	return result, nil
}

// Flush writes all buffered documents in one bulk write and then resets the buffer.
func (bb *BufferedBulkInserter) Flush() (*mongo.BulkWriteResult, error) {
	if bb.docCount == 0 {
		return nil, nil
	}

	defer bb.resetBulk()
	if bb.retryPolicy == nil {
		return bb.bulkWrite(context.Background(), bb.writeModels, bb.bulkWriteOpts)
	}
	return bb.flushWithRetry()
}

func (bb *BufferedBulkInserter) flushWithRetry() (*mongo.BulkWriteResult, error) {
	policy := bb.retryPolicy
	pendingModels := bb.writeModels
	result, err := bb.bulkWrite(policy.ctx, pendingModels, bb.bulkWriteOpts)
	if err == nil {
		return result, err
	}

	nextModels, retryable := bb.modelsForRetry(pendingModels, err)
	if !retryable {
		return result, err
	}

	combinedResult, lastErr := mergeBulkWriteResults(nil, result), err
	pendingModels = nextModels
	retryCtx := policy.ctx
	cancel := func() {}
	if policy.timeout > 0 {
		retryCtx, cancel = context.WithTimeout(policy.ctx, policy.timeout)
	}
	defer cancel()

	for attempt := 0; ; attempt++ {
		if ctxErr := retryCtx.Err(); ctxErr != nil {
			return combinedResult, bulkWriteRetryContextError(ctxErr, policy.timeout, lastErr)
		}

		delay := policy.retryDelay(attempt)
		log.Logvf(log.Always,
			"retrying %v model(s) after retryable bulk write error (attempt %v, next retry in %v): %v",
			len(pendingModels), attempt+1, delay, lastErr)
		if waitErr := waitForBulkWriteRetry(retryCtx, delay); waitErr != nil {
			return combinedResult, bulkWriteRetryContextError(waitErr, policy.timeout, lastErr)
		}

		result, err = bb.bulkWrite(retryCtx, pendingModels, bb.bulkWriteOpts)
		combinedResult = mergeBulkWriteResults(combinedResult, result)
		if err == nil {
			return combinedResult, nil
		}

		nextModels, retryable = bb.modelsForRetry(pendingModels, err)
		if ctxErr := retryCtx.Err(); ctxErr != nil {
			return combinedResult, bulkWriteRetryContextError(ctxErr, policy.timeout, err)
		}
		if !retryable {
			return combinedResult, err
		}
		pendingModels, lastErr = nextModels, err
	}
}

// modelsForRetry returns only models whose outcome is known to be unsuccessful. A command-level
// retry policy cannot identify individual outcomes, so it retains the complete batch. For a
// BulkWriteException, every reported write error must be accepted by shouldRetryWriteError.
func (bb *BufferedBulkInserter) modelsForRetry(
	models []mongo.WriteModel,
	err error,
) ([]mongo.WriteModel, bool) {
	policy := bb.retryPolicy
	if policy.shouldRetry(err) {
		return models, true
	}

	bulkErr, ok := err.(mongo.BulkWriteException)
	if !ok || policy.shouldRetryWriteError == nil || bulkErr.WriteConcernError != nil ||
		len(bulkErr.WriteErrors) == 0 {
		return nil, false
	}

	retryIndexes := make([]bool, len(models))
	firstRetryIndex := len(models)
	for _, writeErr := range bulkErr.WriteErrors {
		if !policy.shouldRetryWriteError(writeErr.WriteError) ||
			writeErr.Index < 0 || writeErr.Index >= len(models) {
			return nil, false
		}
		retryIndexes[writeErr.Index] = true
		if writeErr.Index < firstRetryIndex {
			firstRetryIndex = writeErr.Index
		}
	}

	ordered := bb.bulkWriteOpts != nil && bb.bulkWriteOpts.Ordered != nil &&
		*bb.bulkWriteOpts.Ordered
	if ordered {
		// In an ordered write, models after the first error were not attempted.
		return models[firstRetryIndex:], true
	}

	retryModels := make([]mongo.WriteModel, 0, len(bulkErr.WriteErrors))
	for index, retry := range retryIndexes {
		if retry {
			retryModels = append(retryModels, models[index])
		}
	}
	return retryModels, len(retryModels) > 0
}

func mergeBulkWriteResults(total, next *mongo.BulkWriteResult) *mongo.BulkWriteResult {
	if next == nil {
		return total
	}
	if total == nil {
		total = &mongo.BulkWriteResult{UpsertedIDs: make(map[int64]interface{})}
	}

	total.InsertedCount += next.InsertedCount
	total.MatchedCount += next.MatchedCount
	total.ModifiedCount += next.ModifiedCount
	total.DeletedCount += next.DeletedCount
	total.UpsertedCount += next.UpsertedCount
	for index, id := range next.UpsertedIDs {
		total.UpsertedIDs[index] = id
	}
	return total
}

func defaultBulkWriteRetryDelay(attempt int) time.Duration {
	minDelay := 30 * time.Second
	maxDelay := 60 * time.Second
	// attempt is zero-based. Give a bulk that has already retried ten times
	// a narrower window so it is less likely to be overtaken by newer work.
	if attempt >= 10 {
		maxDelay = 50 * time.Second
	}

	return minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay)+1))
}

func waitForBulkWriteRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func bulkWriteRetryContextError(ctxErr error, timeout time.Duration, lastErr error) error {
	if ctxErr == context.DeadlineExceeded && timeout > 0 {
		return fmt.Errorf("timed out retrying bulk write after %v: %w", timeout, lastErr)
	}
	return fmt.Errorf("bulk write retry canceled: %w", ctxErr)
}
