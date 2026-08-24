// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"context"
	"testing"

	"github.com/mongodb/mongo-tools/common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestIsEloqOutOfMemoryError(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	targetErr := mongo.CommandError{
		Code:    eloqOutOfMemoryCode,
		Name:    eloqOutOfMemoryName,
		Message: "TxError[22]: Transaction failed due to out of memory.",
	}

	Convey("The Eloq out-of-memory matcher", t, func() {
		Convey("matches the exact command error", func() {
			So(isEloqOutOfMemoryError(targetErr), ShouldBeTrue)
		})

		Convey("requires the expected error code", func() {
			err := targetErr
			err.Code++
			So(isEloqOutOfMemoryError(err), ShouldBeFalse)
		})

		Convey("requires the expected error name", func() {
			err := targetErr
			err.Name = "OtherError"
			So(isEloqOutOfMemoryError(err), ShouldBeFalse)
		})

		Convey("requires the Eloq transaction error marker", func() {
			err := targetErr
			err.Message = "Transaction failed due to out of memory."
			So(isEloqOutOfMemoryError(err), ShouldBeFalse)
		})

		Convey("does not match a bulk write error with the same code and message", func() {
			err := mongo.BulkWriteException{
				WriteErrors: []mongo.BulkWriteError{{
					WriteError: mongo.WriteError{
						Code:    eloqOutOfMemoryCode,
						Message: targetErr.Message,
					},
				}},
			}
			So(isEloqOutOfMemoryError(err), ShouldBeFalse)
		})

		Convey("does not match a write concern error with the same code and message", func() {
			err := mongo.BulkWriteException{
				WriteConcernError: &mongo.WriteConcernError{
					Code:    eloqOutOfMemoryCode,
					Message: targetErr.Message,
				},
			}
			So(isEloqOutOfMemoryError(err), ShouldBeFalse)
		})
	})
}

func TestIsEloqOutOfMemoryWriteError(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	targetErr := mongo.WriteError{
		Code:    eloqOutOfMemoryCode,
		Message: "TxError[22]: Transaction failed due to out of memory.",
	}

	Convey("The indexed Eloq out-of-memory matcher", t, func() {
		Convey("matches the expected code and transaction marker", func() {
			So(isEloqOutOfMemoryWriteError(targetErr), ShouldBeTrue)
		})

		Convey("requires the expected numeric code", func() {
			err := targetErr
			err.Code++
			So(isEloqOutOfMemoryWriteError(err), ShouldBeFalse)
		})

		Convey("requires the Eloq transaction marker", func() {
			err := targetErr
			err.Message = "Transaction failed due to out of memory."
			So(isEloqOutOfMemoryWriteError(err), ShouldBeFalse)
		})
	})
}

func TestHandleInterruptCancelsRetryContext(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("Handling an interrupt cancels in-flight retry work", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		restore := &MongoRestore{ctx: ctx, cancel: cancel}

		restore.HandleInterrupt()

		So(restore.terminate, ShouldBeTrue)
		So(ctx.Err(), ShouldEqual, context.Canceled)
	})
}
