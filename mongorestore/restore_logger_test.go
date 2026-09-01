// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/testtype"
	"github.com/stretchr/testify/require"
)

func TestRestoreLogger(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	path := filepath.Join(t.TempDir(), "mongorestore.log")
	logger, err := newRestoreLogger(path)
	require.NoError(t, err, "create the restore logger")
	require.Equal(t, restoreLogMaxSizeMB, logger.output.MaxSize, "configure the maximum log size")
	require.Equal(t, restoreLogMaxBackups, logger.output.MaxBackups, "configure the number of backups")
	require.Equal(t, restoreLogMaxAgeDays, logger.output.MaxAge, "configure backup retention")
	require.True(t, logger.output.Compress, "compress rotated logs")
	require.Equal(t, time.Minute, restoreLogProgressDelay, "configure the progress interval")

	byteProgress := progress.NewCounter(2048)
	byteProgress.Set(1024)
	collectionProgress := newCollectionRestoreProgress(logger, "test.users", 2048, 2, byteProgress)
	collectionProgress.addResult(Result{Successes: 20, Failures: 1})
	collectionProgress.logProgress()
	collectionProgress.logRetryEvent(2, db.BulkWriteRetryEvent{
		Type:      db.BulkWriteRetryScheduled,
		Attempt:   1,
		Documents: 1000,
		Bytes:     1024,
		Delay:     52 * time.Second,
		Elapsed:   time.Second,
		Timeout:   15 * time.Minute,
		Error:     errors.New("ExceededMemoryLimit: transaction ran out of memory"),
	})
	collectionProgress.finish(Result{Successes: 20, Failures: 1})
	require.NoError(t, logger.Close(), "close the restore logger")

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "read the restore log")
	logText := string(contents)
	require.Contains(t, logText, "Collection restore started", "log collection start")
	require.Contains(t, logText, "Restore progress", "log periodic progress")
	require.Contains(t, logText, "OOM bulk write retry scheduled", "log OOM retry details")
	require.Contains(t, logText, "Collection restore completed", "log collection completion")
	require.Contains(t, logText, "namespace=test.users", "include the namespace")
	require.Contains(t, logText, "batch_documents=1000", "include the retry batch size")
	require.Contains(t, logText, "next_retry=52s", "include the retry delay")
	require.Contains(t, logText, "ExceededMemoryLimit: transaction ran out of memory", "include the retry error")
	require.False(t, strings.HasPrefix(strings.TrimSpace(logText), "{"), "use human-readable text rather than JSON")
}

func TestNewRestoreLoggerRejectsInvalidPath(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	_, err := newRestoreLogger(filepath.Join(t.TempDir(), "missing", "mongorestore.log"))
	require.Error(t, err, "reject a path whose parent directory does not exist")
}
