// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/mongo-tools/common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func newRetryTestBufferedBulkInserter(
	ctx context.Context,
	timeout time.Duration,
	docLimit int,
	shouldRetry func(error) bool,
	write bulkWriteFunc,
) *BufferedBulkInserter {
	bb := &BufferedBulkInserter{
		writeModels:   make([]mongo.WriteModel, 0, docLimit),
		docLimit:      docLimit,
		byteLimit:     MAX_MESSAGE_SIZE_BYTES - 100,
		bulkWriteOpts: mongooptions.BulkWrite(),
		bulkWrite:     write,
	}
	bb.SetRetryPolicy(ctx, timeout, shouldRetry)
	bb.retryPolicy.retryDelay = func(int) time.Duration { return 0 }
	return bb
}

func TestBufferedBulkInserterRetry(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("BufferedBulkInserter retry behavior", t, func() {
		retryErr := errors.New("retryable error")
		shouldRetry := func(err error) bool { return err == retryErr }
		rawDoc, err := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
		So(err, ShouldBeNil)

		Convey("retries the same buffered models until the write succeeds", func() {
			calls := 0
			var firstModel mongo.WriteModel
			write := func(ctx context.Context, models []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				_, hasDeadline := ctx.Deadline()
				So(hasDeadline, ShouldEqual, calls > 1)
				So(len(models), ShouldEqual, 1)
				if firstModel == nil {
					firstModel = models[0]
				} else {
					So(models[0], ShouldEqual, firstModel)
				}
				if calls < 3 {
					return &mongo.BulkWriteResult{}, retryErr
				}
				return &mongo.BulkWriteResult{InsertedCount: 1}, nil
			}

			bb := newRetryTestBufferedBulkInserter(context.Background(), time.Minute, 1, shouldRetry, write)
			result, insertErr := bb.InsertRaw(rawDoc)

			So(insertErr, ShouldBeNil)
			So(result.InsertedCount, ShouldEqual, 1)
			So(calls, ShouldEqual, 3)
			So(bb.docCount, ShouldEqual, 0)
			So(bb.byteCount, ShouldEqual, 0)
			So(len(bb.writeModels), ShouldEqual, 0)
		})

		Convey("does not retry an error rejected by the policy", func() {
			nonRetryErr := errors.New("not retryable")
			calls := 0
			write := func(_ context.Context, _ []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				return &mongo.BulkWriteResult{}, nonRetryErr
			}

			bb := newRetryTestBufferedBulkInserter(context.Background(), time.Minute, 1, shouldRetry, write)
			_, insertErr := bb.InsertRaw(rawDoc)

			So(insertErr, ShouldEqual, nonRetryErr)
			So(calls, ShouldEqual, 1)
			So(bb.docCount, ShouldEqual, 0)
			So(len(bb.writeModels), ShouldEqual, 0)
		})

		Convey("stops at the retry deadline and preserves the last retryable error", func() {
			calls := 0
			write := func(_ context.Context, _ []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				return &mongo.BulkWriteResult{}, retryErr
			}

			bb := newRetryTestBufferedBulkInserter(context.Background(), time.Nanosecond, 1, shouldRetry, write)
			bb.retryPolicy.retryDelay = func(int) time.Duration { return time.Hour }
			_, insertErr := bb.InsertRaw(rawDoc)

			So(insertErr, ShouldNotBeNil)
			So(strings.Contains(insertErr.Error(), "timed out retrying bulk write after 1ns"), ShouldBeTrue)
			So(errors.Is(insertErr, retryErr), ShouldBeTrue)
			So(calls, ShouldEqual, 1)
			So(bb.docCount, ShouldEqual, 0)
		})

		Convey("stops promptly when its context is canceled", func() {
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			write := func(_ context.Context, _ []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				cancel()
				return &mongo.BulkWriteResult{}, retryErr
			}

			bb := newRetryTestBufferedBulkInserter(ctx, 0, 1, shouldRetry, write)
			_, insertErr := bb.InsertRaw(rawDoc)

			So(errors.Is(insertErr, context.Canceled), ShouldBeTrue)
			So(calls, ShouldEqual, 1)
			So(bb.docCount, ShouldEqual, 0)
		})

		Convey("a zero timeout does not impose an attempt limit", func() {
			calls := 0
			write := func(_ context.Context, _ []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				if calls < 8 {
					return &mongo.BulkWriteResult{}, retryErr
				}
				return &mongo.BulkWriteResult{InsertedCount: 1}, nil
			}

			bb := newRetryTestBufferedBulkInserter(context.Background(), 0, 1, shouldRetry, write)
			result, insertErr := bb.InsertRaw(rawDoc)

			So(insertErr, ShouldBeNil)
			So(result.InsertedCount, ShouldEqual, 1)
			So(calls, ShouldEqual, 8)
		})
	})
}

func TestBufferedBulkInserterRetryBatchBounds(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("A retry-enabled inserter reserves generated _id overhead before flushing", t, func() {
		batchSizes := []int{}
		write := func(_ context.Context, models []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			batchSizes = append(batchSizes, len(models))
			return &mongo.BulkWriteResult{InsertedCount: int64(len(models))}, nil
		}
		bb := newRetryTestBufferedBulkInserter(
			context.Background(), time.Minute, 2000, func(error) bool { return false }, write)

		expectedByteLimit := MaxBSONSize - maxRetryableBulkDocuments*generatedObjectIDOverhead
		So(bb.docLimit, ShouldEqual, maxRetryableBulkDocuments)
		So(bb.byteLimit, ShouldEqual, expectedByteLimit)

		for i := 0; i < maxRetryableBulkDocuments-1; i++ {
			result, err := bb.InsertRaw([]byte{1})
			So(err, ShouldBeNil)
			So(result, ShouldBeNil)
		}

		lastDoc := make([]byte, expectedByteLimit-bb.byteCount+1)
		worstCaseSize := bb.byteCount + len(lastDoc) + maxRetryableBulkDocuments*generatedObjectIDOverhead
		So(worstCaseSize, ShouldBeGreaterThan, MaxBSONSize)

		result, err := bb.InsertRaw(lastDoc)
		So(err, ShouldBeNil)
		So(result.InsertedCount, ShouldEqual, maxRetryableBulkDocuments-1)
		So(batchSizes, ShouldResemble, []int{maxRetryableBulkDocuments - 1})
		So(bb.docCount, ShouldEqual, 1)
		So(bb.byteCount, ShouldEqual, len(lastDoc))

		result, err = bb.Flush()
		So(err, ShouldBeNil)
		So(result.InsertedCount, ShouldEqual, 1)
		So(batchSizes, ShouldResemble, []int{maxRetryableBulkDocuments - 1, 1})
		So(bb.docCount, ShouldEqual, 0)
	})

	Convey("A retry-enabled inserter caps a custom document limit", t, func() {
		batchSizes := []int{}
		write := func(_ context.Context, models []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
			batchSizes = append(batchSizes, len(models))
			return &mongo.BulkWriteResult{InsertedCount: int64(len(models))}, nil
		}
		bb := newRetryTestBufferedBulkInserter(
			context.Background(), time.Minute, 2000, func(error) bool { return false }, write)

		for i := 0; i < maxRetryableBulkDocuments+1; i++ {
			_, err := bb.InsertRaw([]byte{1})
			So(err, ShouldBeNil)
		}
		So(batchSizes, ShouldResemble, []int{maxRetryableBulkDocuments})
		So(bb.docCount, ShouldEqual, 1)

		_, err := bb.Flush()
		So(err, ShouldBeNil)
		So(batchSizes, ShouldResemble, []int{maxRetryableBulkDocuments, 1})
	})

	Convey("An inserter without a retry policy retains its existing byte-limit behavior", t, func() {
		calls := 0
		bb := &BufferedBulkInserter{
			writeModels:   make([]mongo.WriteModel, 0, 1000),
			docLimit:      1000,
			byteLimit:     1,
			bulkWriteOpts: mongooptions.BulkWrite(),
			bulkWrite: func(_ context.Context, models []mongo.WriteModel, _ ...*mongooptions.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
				calls++
				return &mongo.BulkWriteResult{InsertedCount: int64(len(models))}, nil
			},
		}

		result, err := bb.InsertRaw([]byte{1, 2})
		So(err, ShouldBeNil)
		So(result.InsertedCount, ShouldEqual, 1)
		So(calls, ShouldEqual, 1)
		So(bb.docCount, ShouldEqual, 0)
	})
}

func TestDefaultBulkWriteRetryDelay(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("The retry delay follows the bounded schedule with small jitter", t, func() {
		tests := []struct {
			attempt int
			min     time.Duration
			max     time.Duration
		}{
			{0, 9 * time.Second, 11 * time.Second},
			{1, 18 * time.Second, 22 * time.Second},
			{2, 36 * time.Second, 44 * time.Second},
			{3, 54 * time.Second, 60 * time.Second},
			{8, 54 * time.Second, 60 * time.Second},
		}

		for _, test := range tests {
			delay := defaultBulkWriteRetryDelay(test.attempt)
			So(delay >= test.min, ShouldBeTrue)
			So(delay <= test.max, ShouldBeTrue)
		}
	})
}
