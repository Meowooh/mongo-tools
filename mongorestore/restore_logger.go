// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/text"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	restoreLogMaxSizeMB     = 100
	restoreLogMaxBackups    = 10
	restoreLogMaxAgeDays    = 7
	restoreLogProgressDelay = time.Minute
)

type restoreLogger struct {
	logger *logrus.Logger
	output *lumberjack.Logger
}

func newRestoreLogger(path string) (*restoreLogger, error) {
	if path == "" {
		return nil, nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}

	output := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    restoreLogMaxSizeMB,
		MaxBackups: restoreLogMaxBackups,
		MaxAge:     restoreLogMaxAgeDays,
		Compress:   true,
	}
	logger := logrus.New()
	logger.SetOutput(output)
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05.000-0700",
	})

	return &restoreLogger{logger: logger, output: output}, nil
}

func (logger *restoreLogger) Close() error {
	if logger == nil {
		return nil
	}
	return logger.output.Close()
}

type collectionRestoreProgress struct {
	logger        *restoreLogger
	namespace     string
	workers       int
	byteProgress  *progress.CountProgressor
	startedAt     time.Time
	successes     int64
	failures      int64
	retries       int64
	stop          chan struct{}
	loggerStopped chan struct{}
}

func newCollectionRestoreProgress(
	logger *restoreLogger,
	namespace string,
	totalBytes int64,
	workers int,
	byteProgress *progress.CountProgressor,
) *collectionRestoreProgress {
	if logger == nil {
		return nil
	}

	collectionProgress := &collectionRestoreProgress{
		logger:        logger,
		namespace:     namespace,
		workers:       workers,
		byteProgress:  byteProgress,
		startedAt:     time.Now(),
		stop:          make(chan struct{}),
		loggerStopped: make(chan struct{}),
	}
	logger.logger.WithFields(logrus.Fields{
		"namespace": namespace,
		"size":      text.FormatByteAmount(totalBytes),
		"workers":   workers,
	}).Info("Collection restore started")
	go collectionProgress.run()
	return collectionProgress
}

func (collectionProgress *collectionRestoreProgress) run() {
	ticker := time.NewTicker(restoreLogProgressDelay)
	defer ticker.Stop()
	defer close(collectionProgress.loggerStopped)

	for {
		select {
		case <-ticker.C:
			collectionProgress.logProgress()
		case <-collectionProgress.stop:
			return
		}
	}
}

func (collectionProgress *collectionRestoreProgress) addResult(result Result) {
	if collectionProgress == nil {
		return
	}
	atomic.AddInt64(&collectionProgress.successes, result.Successes)
	atomic.AddInt64(&collectionProgress.failures, result.Failures)
}

func (collectionProgress *collectionRestoreProgress) logProgress() {
	currentBytes, totalBytes := collectionProgress.byteProgress.Progress()
	elapsed := time.Since(collectionProgress.startedAt)
	fields := logrus.Fields{
		"namespace": collectionProgress.namespace,
		"documents": atomic.LoadInt64(&collectionProgress.successes),
		"failures":  atomic.LoadInt64(&collectionProgress.failures),
		"data":      formatByteProgress(currentBytes, totalBytes),
		"elapsed":   formatLogDuration(elapsed),
		"retries":   atomic.LoadInt64(&collectionProgress.retries),
		"workers":   collectionProgress.workers,
	}

	if elapsed > 0 {
		bytesPerSecond := int64(float64(currentBytes) / elapsed.Seconds())
		fields["rate"] = text.FormatByteAmount(bytesPerSecond) + "/s"
		if totalBytes > currentBytes && bytesPerSecond > 0 {
			eta := time.Duration(float64(totalBytes-currentBytes)/float64(bytesPerSecond)) * time.Second
			fields["eta"] = formatLogDuration(eta)
		}
	}
	if totalBytes > 0 {
		fields["progress"] = fmt.Sprintf("%.1f%%", float64(currentBytes)*100/float64(totalBytes))
	}

	collectionProgress.logger.logger.WithFields(fields).Info("Restore progress")
}

func (collectionProgress *collectionRestoreProgress) finish(result Result) {
	if collectionProgress == nil {
		return
	}
	close(collectionProgress.stop)
	<-collectionProgress.loggerStopped

	currentBytes, totalBytes := collectionProgress.byteProgress.Progress()
	entry := collectionProgress.logger.logger.WithFields(logrus.Fields{
		"namespace": collectionProgress.namespace,
		"documents": result.Successes,
		"failures":  result.Failures,
		"data":      formatByteProgress(currentBytes, totalBytes),
		"elapsed":   formatLogDuration(time.Since(collectionProgress.startedAt)),
		"retries":   atomic.LoadInt64(&collectionProgress.retries),
	})
	if result.Err != nil {
		entry.WithError(result.Err).Error("Collection restore failed")
		return
	}
	entry.Info("Collection restore completed")
}

func (collectionProgress *collectionRestoreProgress) retryObserver(worker int) db.BulkWriteRetryObserver {
	if collectionProgress == nil {
		return nil
	}
	return func(event db.BulkWriteRetryEvent) {
		collectionProgress.logRetryEvent(worker, event)
	}
}

func (collectionProgress *collectionRestoreProgress) logRetryEvent(worker int, event db.BulkWriteRetryEvent) {
	fields := logrus.Fields{
		"namespace":       collectionProgress.namespace,
		"worker":          worker,
		"attempt":         event.Attempt,
		"batch_documents": event.Documents,
		"batch_size":      text.FormatByteAmount(int64(event.Bytes)),
		"elapsed":         formatLogDuration(event.Elapsed),
	}
	if event.AttemptDuration > 0 {
		fields["attempt_duration"] = formatLogDuration(event.AttemptDuration)
	}
	if event.Delay > 0 {
		fields["next_retry"] = formatLogDuration(event.Delay)
	}
	if event.Timeout > 0 {
		remaining := event.Timeout - event.Elapsed
		if remaining < 0 {
			remaining = 0
		}
		fields["timeout_remaining"] = formatLogDuration(remaining)
	} else {
		fields["timeout_remaining"] = "unlimited"
	}
	entry := collectionProgress.logger.logger.WithFields(fields)
	if event.Error != nil {
		entry = entry.WithError(event.Error)
	}

	switch event.Type {
	case db.BulkWriteRetryScheduled:
		atomic.AddInt64(&collectionProgress.retries, 1)
		entry.Warn("OOM bulk write retry scheduled")
	case db.BulkWriteRetryAttemptFailed:
		entry.Warn("OOM bulk write retry failed")
	case db.BulkWriteRetrySucceeded:
		entry.Info("OOM bulk write retry succeeded")
	case db.BulkWriteRetryTimedOut:
		entry.Error("OOM bulk write retry timed out")
	case db.BulkWriteRetryCanceled:
		entry.Warn("OOM bulk write retry canceled")
	case db.BulkWriteRetryNonRetryableErr:
		entry.Error("Bulk write retry stopped after non-retryable error")
	}
}

func formatByteProgress(current, total int64) string {
	if total <= 0 {
		return text.FormatByteAmount(current)
	}
	return fmt.Sprintf("%s/%s", text.FormatByteAmount(current), text.FormatByteAmount(total))
}

func formatLogDuration(duration time.Duration) string {
	if duration < time.Second {
		return duration.Round(time.Millisecond).String()
	}
	return duration.Round(time.Second).String()
}
