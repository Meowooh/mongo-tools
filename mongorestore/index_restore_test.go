// Copyright (C) MongoDB, Inc. 2026-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/intents"
	"github.com/mongodb/mongo-tools/common/testtype"
	"github.com/mongodb/mongo-tools/common/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

type indexRestoreMode struct {
	name                string
	args                []string
	indexAfter          bool
	indexesBeforeBSON   []string
	indexesAfterRestore []string
}

func indexRestoreModes() []indexRestoreMode {
	return []indexRestoreMode{
		{
			name:                "default",
			indexesBeforeBSON:   []string{"_id_", "email_unique"},
			indexesAfterRestore: []string{"_id_", "email_unique", "category_1"},
		},
		{
			name:                "no_index_restore",
			args:                []string{NoIndexRestoreOption},
			indexesBeforeBSON:   []string{"_id_", "email_unique"},
			indexesAfterRestore: []string{"_id_", "email_unique"},
		},
		{
			name:                "index_after",
			args:                []string{IndexAfterOption},
			indexAfter:          true,
			indexesBeforeBSON:   []string{"_id_"},
			indexesAfterRestore: []string{"_id_", "email_unique", "category_1"},
		},
		{
			name:                "index_after_no_index_restore",
			args:                []string{IndexAfterOption, NoIndexRestoreOption},
			indexAfter:          true,
			indexesBeforeBSON:   []string{"_id_"},
			indexesAfterRestore: []string{"_id_"},
		},
	}
}

func TestRestoreIntentIndexTiming(t *testing.T) {
	req := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	provider, _, err := testutil.GetBareSessionProvider()
	req.NoError(err, "can connect to the integration test server")
	defer provider.Close()

	for _, lifecycle := range []string{"new", "reuse", "drop"} {
		modes := indexRestoreModes()
		if lifecycle != "new" {
			// Flag combinations are covered once. Collection lifecycle checks
			// only need the default mode that adds the pre-data phase.
			modes = modes[:1]
		}
		for _, mode := range modes {
			t.Run(lifecycle+"/"+mode.name, func(t *testing.T) {
				require := require.New(t)

				collection := indexRestoreTestCollection(t, provider)
				args := append([]string{}, mode.args...)
				if lifecycle == "reuse" {
					// EloqDoc permits reuse of an existing unique index, but not
					// a new unique index build after the collection contains data.
					_, err := collection.Indexes().CreateOne(context.Background(), mongo.IndexModel{
						Keys:    bson.D{{"email", 1}},
						Options: mongooptions.Index().SetName("email_unique").SetUnique(true),
					})
					require.NoError(err, "can create an equivalent unique index before seeding the reused collection")
				}
				var countBeforeBSON int64
				if lifecycle != "new" {
					_, err := collection.InsertOne(context.Background(), bson.D{{"_id", -1}, {"email", "existing"}})
					require.NoError(err, "can seed the existing collection")
					countBeforeBSON = 1
				}
				if lifecycle == "drop" {
					// Both duplicate old data and a conflicting old index must disappear
					// before the unique index from the dump can be created.
					_, err := collection.InsertOne(context.Background(), bson.D{{"_id", -2}, {"email", "existing"}})
					require.NoError(err, "can seed duplicate data that --drop must remove")
					_, err = collection.Indexes().CreateOne(context.Background(), mongo.IndexModel{
						Keys:    bson.D{{"legacy", 1}},
						Options: mongooptions.Index().SetName("email_unique"),
					})
					require.NoError(err, "can seed an index whose name conflicts with the dump")
					args = append(args, DropOption)
					countBeforeBSON = 0
				}

				restore := newIndexRestoreForTest(t, args...)
				intent, file := indexRestoreTestIntent(t, restore, collection, indexRestoreMetadata(true), indexRestoreDocuments(false))
				file.onOpen = func() {
					indexes := indexRestoreListIndexes(t, collection)
					require.ElementsMatch(mode.indexesBeforeBSON, indexRestoreIndexNames(indexes), "only the expected indexes exist before BSON input is opened")
					require.Equal(countBeforeBSON, indexRestoreDocumentCount(t, collection), "collection creation, reuse, or drop finishes before BSON input is opened")
					if !mode.indexAfter {
						require.Equal(bson.D{{"email", int32(1)}}, indexes["email_unique"].Key, "the prebuilt unique index uses the dump key, not the dropped index key")
						require.True(indexes["email_unique"].Unique, "the secondary index is unique before BSON input is opened")
					}
				}

				result := restore.RestoreIntent(intent)
				require.NoError(result.Err, "can restore distinct documents in every index mode")
				require.True(file.opened, "a successful RestoreIntent opens its BSON input")
				require.Equal(int64(2), result.Successes, "both dump documents are restored")
				require.Zero(result.Failures, "distinct documents have no insertion failures")
				require.Equal(countBeforeBSON+2, indexRestoreDocumentCount(t, collection), "RestoreIntent preserves reused data and replaces dropped data")
				require.ElementsMatch(mode.indexesBeforeBSON, indexRestoreIndexNames(indexRestoreListIndexes(t, collection)), "ordinary indexes are still deferred after RestoreIntent")

				err = restore.RestoreIndexes()
				require.NoError(err, "the final index phase can resubmit an already prebuilt unique index")
				require.ElementsMatch(mode.indexesAfterRestore, indexRestoreIndexNames(indexRestoreListIndexes(t, collection)), "the final phase restores ordinary indexes unless --noIndexRestore is set")
			})
		}
	}
}

func TestRestoreIntentUniqueIndexConflictBeforeBSON(t *testing.T) {
	req := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	provider, _, err := testutil.GetBareSessionProvider()
	req.NoError(err, "can connect to the integration test server")
	defer provider.Close()

	for _, noIndexRestore := range []bool{false, true} {
		t.Run(fmt.Sprintf("noIndexRestore=%t", noIndexRestore), func(t *testing.T) {
			require := require.New(t)
			collection := indexRestoreTestCollection(t, provider)
			_, err := collection.InsertOne(context.Background(), bson.D{{"_id", -1}, {"email", "existing"}})
			require.NoError(err, "can seed existing data")
			_, err = collection.Indexes().CreateOne(context.Background(), mongo.IndexModel{
				Keys:    bson.D{{"legacy", 1}},
				Options: mongooptions.Index().SetName("email_unique"),
			})
			require.NoError(err, "can create a conflicting index")
			indexesBefore := indexRestoreListIndexes(t, collection)

			var args []string
			if noIndexRestore {
				args = append(args, NoIndexRestoreOption)
			}
			restore := newIndexRestoreForTest(t, args...)
			intent, file := indexRestoreTestIntent(t, restore, collection, indexRestoreMetadata(true), indexRestoreDocuments(false))

			result := restore.RestoreIntent(intent)
			require.Error(result.Err, "a failed prebuild stops the restore even with --noIndexRestore")
			require.Contains(result.Err.Error(), "email_unique", "the error identifies the conflicting index")
			require.False(file.opened, "a failed prebuild does not open BSON input")
			require.Zero(result.Successes, "a failed prebuild inserts no documents")
			require.Equal(int64(1), indexRestoreDocumentCount(t, collection), "existing data remains intact")
			require.Equal(indexesBefore, indexRestoreListIndexes(t, collection), "existing indexes remain intact")
		})
	}
}

func TestRestoreIntentUniqueIndexWriteErrors(t *testing.T) {
	req := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	provider, _, err := testutil.GetBareSessionProvider()
	req.NoError(err, "can connect to the integration test server")
	defer provider.Close()

	for _, mode := range indexRestoreModes() {
		// Exercise both the buffered-insert and final-flush error paths.
		for _, batchSize := range []string{"1", "1000"} {
			t.Run(mode.name+"/batchSize="+batchSize, func(t *testing.T) {
				require := require.New(t)
				collection := indexRestoreTestCollection(t, provider)
				_, err := collection.Indexes().CreateOne(context.Background(), mongo.IndexModel{
					Keys:    bson.D{{"email", 1}},
					Options: mongooptions.Index().SetName("email_unique").SetUnique(true),
				})
				require.NoError(err, "an existing unique index rejects duplicates in either restore order")
				args := append([]string{BulkBufferSizeOption, batchSize}, mode.args...)
				restore := newIndexRestoreForTest(t, args...)
				intent, _ := indexRestoreTestIntent(t, restore, collection, indexRestoreMetadata(true), indexRestoreDocuments(true))

				result := restore.RestoreIntent(intent)
				if mode.indexAfter {
					require.NoError(result.Err, "--indexAfter preserves the original continue-on-error policy")
				} else {
					require.Error(result.Err, "the default mode treats unique constraint violations as fatal")
					require.Contains(result.Err.Error(), "duplicate key", "the restore reports the duplicate-key error")
				}
				require.Equal(int64(1), result.Successes, "one document satisfies the unique constraint")
				require.Equal(int64(1), result.Failures, "the duplicate document is reported")
				require.Equal(int64(1), indexRestoreDocumentCount(t, collection), "the unique index rejects the duplicate")
				require.False(restore.OutputOptions.StopOnError, "the per-collection policy does not mutate the shared option")
			})
		}
	}
}

func TestRestoreUniqueIndexesFromTemporaryDump(t *testing.T) {
	require := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	provider, _, err := testutil.GetBareSessionProvider()
	require.NoError(err, "can connect to the integration test server")
	t.Cleanup(provider.Close)

	collection := indexRestoreTestCollection(t, provider)
	restore := newIndexRestoreForTest(t, NoIndexRestoreOption)
	restore.TargetDirectory = indexRestoreTemporaryDump(t, collection, indexRestoreMetadata(true), indexRestoreDocuments(false))

	result := restore.Restore()
	require.NoError(result.Err, "the full restore creates unique indexes even with --noIndexRestore")
	require.Equal(int64(2), result.Successes, "the full restore imports both documents")
	require.Zero(result.Failures, "the full restore has no insertion failures")
	require.Equal(int64(2), indexRestoreDocumentCount(t, collection), "both documents reach the target")
	require.ElementsMatch([]string{"_id_", "email_unique"}, indexRestoreIndexNames(indexRestoreListIndexes(t, collection)),
		"metadata reaches the pre-data phase while ordinary indexes remain skipped")
}

// indexRestoreFile implements the file interface used by intents.Intent. Its
// Open hook observes the database before any BSON documents can be read.
type indexRestoreFile struct {
	pos int64 // accessed by the BSON reader and insertion workers
	*bytes.Reader
	opened bool
	onOpen func()
}

func newIndexRestoreFile(data []byte) *indexRestoreFile {
	return &indexRestoreFile{Reader: bytes.NewReader(data)}
}

func (file *indexRestoreFile) Open() error {
	file.opened = true
	if file.onOpen != nil {
		file.onOpen()
	}
	return nil
}

func (file *indexRestoreFile) Read(data []byte) (int, error) {
	if !file.opened {
		return 0, fmt.Errorf("test input must be opened before it is read")
	}
	n, err := file.Reader.Read(data)
	atomic.AddInt64(&file.pos, int64(n))
	return n, err
}

func (*indexRestoreFile) Write([]byte) (int, error) {
	return 0, fmt.Errorf("test input is read-only")
}

func (*indexRestoreFile) Close() error { return nil }

func (file *indexRestoreFile) Pos() int64 { return atomic.LoadInt64(&file.pos) }

func newIndexRestoreForTest(t *testing.T, args ...string) *MongoRestore {
	require := require.New(t)
	t.Helper()

	dir, cleanup := testutil.MakeTempDir(t)
	t.Cleanup(cleanup)
	baseArgs := []string{
		RestoreLogPathOption, filepath.Join(dir, "restore.log"),
		NumParallelCollectionsOption, "1",
		NumInsertionWorkersOption, "1",
	}
	restore, err := getRestoreWithArgs(append(baseArgs, args...)...)
	require.NoError(err, "can construct mongorestore with the test's index restore options")
	t.Cleanup(restore.Close)
	return restore
}

func indexRestoreTestCollection(t *testing.T, provider *db.SessionProvider) *mongo.Collection {
	require := require.New(t)
	t.Helper()

	session, err := provider.GetSession()
	require.NoError(err, "can get a session for the integration test server")
	database := session.Database("index_restore_" + primitive.NewObjectID().Hex())
	t.Cleanup(func() {
		require.NoError(database.Drop(context.Background()), "can remove only this test's unique database")
	})
	return database.Collection("items")
}

func indexRestoreTestIntent(t *testing.T, restore *MongoRestore, collection *mongo.Collection, metadata bson.D, documents []bson.D) (*intents.Intent, *indexRestoreFile) {
	require := require.New(t)
	t.Helper()

	require.NoError(restore.ParseAndValidateOptions(), "the direct RestoreIntent test uses valid options")
	var data []byte
	for _, document := range documents {
		raw, err := bson.Marshal(document)
		require.NoError(err, "can encode a test BSON document")
		data = append(data, raw...)
	}
	metadataJSON, err := bson.MarshalExtJSON(metadata, true, false)
	require.NoError(err, "can encode the dump index metadata")
	file := newIndexRestoreFile(data)
	intent := &intents.Intent{
		DB:               collection.Database().Name(),
		C:                collection.Name(),
		BSONFile:         file,
		BSONSize:         int64(len(data)),
		Size:             int64(len(data)),
		Location:         "memory.bson",
		MetadataFile:     newIndexRestoreFile(metadataJSON),
		MetadataLocation: "memory.metadata.json",
	}
	restore.manager = intents.NewIntentManager()
	restore.manager.Put(intent)
	require.NoError(restore.PopulateMetadataForIntents(), "can populate the index catalog through the normal metadata path")
	return intent, file
}

func indexRestoreMetadata(unique bool) bson.D {
	indexes := bson.A{
		bson.D{{"v", int32(2)}, {"key", bson.D{{"_id", int32(1)}}}, {"name", "_id_"}, {"ns", "source.items"}},
		bson.D{{"v", int32(2)}, {"key", bson.D{{"category", int32(1)}}}, {"name", "category_1"}, {"ns", "source.items"}, {"unique", false}},
	}
	if unique {
		indexes = append(indexes, bson.D{
			{"v", int32(2)}, {"key", bson.D{{"email", int32(1)}}},
			{"name", "email_unique"}, {"ns", "source.items"}, {"unique", true},
		})
	}
	return bson.D{{"options", bson.D{}}, {"indexes", indexes}}
}

func indexRestoreDocuments(duplicate bool) []bson.D {
	secondEmail := "second@example.com"
	if duplicate {
		secondEmail = "first@example.com"
	}
	return []bson.D{
		{{"_id", int32(1)}, {"email", "first@example.com"}, {"category", "same"}},
		{{"_id", int32(2)}, {"email", secondEmail}, {"category", "same"}},
	}
}

type indexRestoreIndexSpec struct {
	Name   string `bson:"name"`
	Key    bson.D `bson:"key"`
	Unique bool   `bson:"unique"`
}

func indexRestoreListIndexes(t *testing.T, collection *mongo.Collection) map[string]indexRestoreIndexSpec {
	require := require.New(t)
	t.Helper()

	cursor, err := collection.Indexes().List(context.Background())
	require.NoError(err, "can list the target collection's indexes at this restore phase")
	defer cursor.Close(context.Background())
	var specs []indexRestoreIndexSpec
	require.NoError(cursor.All(context.Background(), &specs), "can decode the complete listIndexes result")
	indexes := make(map[string]indexRestoreIndexSpec, len(specs))
	for _, spec := range specs {
		indexes[spec.Name] = spec
	}
	return indexes
}

func indexRestoreIndexNames(indexes map[string]indexRestoreIndexSpec) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	return names
}

func indexRestoreDocumentCount(t *testing.T, collection *mongo.Collection) int64 {
	require := require.New(t)
	t.Helper()

	count, err := collection.CountDocuments(context.Background(), bson.D{})
	require.NoError(err, "can count target documents at this restore phase")
	return count
}

func indexRestoreTemporaryDump(t *testing.T, collection *mongo.Collection, metadata bson.D, documents []bson.D) string {
	require := require.New(t)
	t.Helper()

	dir, cleanup := testutil.MakeTempDir(t)
	t.Cleanup(cleanup)
	fixture := testCollData{
		ns:       collection.Database().Name() + "." + collection.Name(),
		docs:     documents,
		metadata: metadata,
	}
	require.NoError(fixture.Mkdir(dir), "can create a database directory in a temporary dump")
	require.NoError(fixture.WriteDocs(dir), "can write BSON input without changing existing testdata")
	require.NoError(fixture.WriteMetadata(dir), "can write index metadata without changing existing testdata")
	return dir
}
