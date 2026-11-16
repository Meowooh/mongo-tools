// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/testtype"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestIndexAfterOptionParsing(t *testing.T) {
	assertions := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	assertions.Equal("--indexAfter", IndexAfterOption, "the compatibility flag uses the requested camelCase spelling")
	for _, tc := range []struct {
		name       string
		args       []string
		indexAfter bool
		noIndexes  bool
	}{
		{name: "default"},
		{name: "skip post-data indexes", args: []string{NoIndexRestoreOption}, noIndexes: true},
		{name: "original order", args: []string{IndexAfterOption}, indexAfter: true},
		{name: "original skip all", args: []string{IndexAfterOption, NoIndexRestoreOption}, indexAfter: true, noIndexes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			opts, err := ParseOptions(tc.args, "", "")
			require.NoError(err, "index restore options parse successfully")
			require.Equal(tc.indexAfter, opts.OutputOptions.IndexAfter, "only --indexAfter enables original behavior")
			require.Equal(tc.noIndexes, opts.OutputOptions.NoIndexRestore, "the post-data skip flag is preserved independently")
		})
	}
}

func TestPreDataUniqueIndexesRejectOplog(t *testing.T) {
	require := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	for _, noIndexes := range []bool{false, true} {
		restore := &MongoRestore{
			InputOptions:  &InputOptions{OplogReplay: true},
			OutputOptions: &OutputOptions{NoIndexRestore: noIndexes},
		}
		// There is deliberately no provider: reject before inspecting the
		// server, opening a dump, or changing any collection.
		err := restore.ParseAndValidateOptions()
		require.ErrorContains(err, "--oplogReplay", "oplog replay is not supported with prebuilt unique indexes")
		require.ErrorContains(err, "--indexAfter", "the error explains how to select the original order")
	}
}

func TestUniqueIndexTruthiness(t *testing.T) {
	assertions := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	zero, err := primitive.ParseDecimal128("0")
	assertions.NoError(err, "zero decimal is valid")
	nonzero, err := primitive.ParseDecimal128("18446744073709551616")
	assertions.NoError(err, "a decimal larger than int64 is valid")
	for _, tc := range []struct {
		name  string
		value interface{}
		want  bool
	}{
		{"missing", nil, false},
		{"false", false, false},
		{"true", true, true},
		{"integer zero", int32(0), false},
		{"integer one", int32(1), true},
		{"int zero", int(0), false},
		{"negative", int64(-1), true},
		{"floating zero", float64(0), false},
		{"floating nonzero", 0.25, true},
		{"decimal zero", zero, false},
		{"large decimal", nonzero, true},
		{"undefined", primitive.Undefined{}, false},
		{"legacy string", "true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			index := &idx.IndexDocument{Options: bson.M{"unique": tc.value}}
			require.Equal(tc.want, isUniqueIndex(index), "unique classification follows BSON truthiness")
		})
	}
}

func TestPrepareIndexesPreservesDefinitions(t *testing.T) {
	require := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	unique := &idx.IndexDocument{
		Key: bson.D{{"tenant", int32(1)}, {"email", int32(-1)}},
		Options: bson.M{
			"name": "email_unique", "unique": true, "v": int32(2), "ns": "source.people",
			"collation": bson.D{{"locale", "en"}, {"strength", int32(2)}},
		},
		PartialFilterExpression: bson.D{{"active", true}},
	}
	restore := &MongoRestore{
		OutputOptions: &OutputOptions{},
		indexCatalog:  idx.NewIndexCatalog(),
		serverVersion: db.Version{4, 4, 0},
	}
	restore.indexCatalog.SetCollation("target", "people", false)
	restore.indexCatalog.AddIndexes("target", "people", []*idx.IndexDocument{
		{Key: bson.D{{"_id", int32(1)}}, Options: bson.M{"name": "_id_", "unique": true}},
		unique,
		{Key: bson.D{{"age", int32(1)}}, Options: bson.M{"name": "age_1"}},
	})

	first, err := restore.prepareIndexesForNamespace("target", "people")
	require.NoError(err, "index specs are prepared successfully")
	require.Len(first, 2, "the _id index is handled only by collection creation")
	require.Equal("age_1", first[0].Options["name"], "preparation uses a stable order")
	require.Equal(bson.D{{"locale", "simple"}}, first[0].Options["collation"], "implicit simple index collation is preserved")
	require.Equal(unique, first[1], "compound keys, partial filters, collation, and options are preserved")

	delete(first[1].Options, "v")
	first[1].Options["ns"] = "target.people"
	first[1].Key[0].Value = int32(-1)
	second, err := restore.prepareIndexesForNamespace("target", "people")
	require.NoError(err, "the same catalog can be prepared again")
	require.Equal(unique, second[1], "pre-data processing cannot alter the post-data definitions")

	restore.OutputOptions.NoIndexRestore = true
	uniqueOnly, err := restore.prepareIndexesForNamespace("target", "people")
	require.NoError(err, "--noIndexRestore still permits preparation of unique indexes")
	require.Len(uniqueOnly, 1, "only unique indexes remain when post-data restoration is skipped")
	require.Equal(unique, uniqueOnly[0], "the unique constraint is retained under --noIndexRestore")
}

func TestPrepareIndexesConversions(t *testing.T) {
	require := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	restore := &MongoRestore{
		OutputOptions: &OutputOptions{ConvertLegacyIndexes: true, FixDottedHashedIndexes: true},
		indexCatalog:  idx.NewIndexCatalog(),
		serverVersion: db.Version{4, 0, 0},
	}
	restore.indexCatalog.SetCollation("db", "coll", true)
	restore.indexCatalog.AddIndex("db", "coll", &idx.IndexDocument{
		Key:     bson.D{{"nested.key", "hashed"}, {"legacy", false}},
		Options: bson.M{"name": "unique_legacy", "unique": int32(1), "sparse": true, "invalidOption": true},
	})
	indexes, err := restore.prepareIndexesForNamespace("db", "coll")
	require.NoError(err, "legacy and dotted-hash conversion are available before data")
	require.Len(indexes, 1, "the unique index survives conversion")
	require.Equal(bson.D{{"nested.key", int32(1)}, {"legacy", int32(1)}}, indexes[0].Key, "both key conversions are applied")
	require.NotContains(indexes[0].Options, "invalidOption", "legacy invalid options are removed")
	require.Equal(true, indexes[0].Options["sparse"], "sparse semantics are preserved")
	require.True(isUniqueIndex(indexes[0]), "numeric unique metadata is recognized")
	again, err := restore.prepareIndexesForNamespace("db", "coll")
	require.NoError(err, "conversion can be repeated for the final phase")
	require.Equal(indexes, again, "both phases use identical converted specs")
}

func TestUniqueIndexConversionCollisions(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	for _, tc := range []struct {
		name    string
		options OutputOptions
		key     interface{}
	}{
		{"legacy", OutputOptions{ConvertLegacyIndexes: true}, false},
		{"dotted hash", OutputOptions{FixDottedHashedIndexes: true}, "hashed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			restore := &MongoRestore{OutputOptions: &tc.options, indexCatalog: idx.NewIndexCatalog()}
			restore.indexCatalog.SetCollation("db", "coll", true)
			restore.indexCatalog.AddIndexes("db", "coll", []*idx.IndexDocument{
				{Key: bson.D{{"nested.key", tc.key}}, Options: bson.M{"name": "converted", "unique": true}},
				{Key: bson.D{{"nested.key", int32(1)}}, Options: bson.M{"name": "existing_pattern"}},
			})
			_, err := restore.prepareIndexesForNamespace("db", "coll")
			require.ErrorContains(err, "unique constraint", "conversion cannot silently drop or merge a unique constraint")
			tc.options.IndexAfter = true
			_, err = restore.prepareIndexesForNamespace("db", "coll")
			require.NoError(err, "--indexAfter retains the original conversion behavior")
		})
	}
}

func TestPreDataUniqueIndexNoOp(t *testing.T) {
	require := require.New(t)
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	restore := &MongoRestore{OutputOptions: &OutputOptions{IndexAfter: true}}
	hasUnique, err := restore.restoreUniqueIndexes("db", "coll")
	require.NoError(err, "--indexAfter does not inspect metadata or contact the server before data")
	require.False(hasUnique, "no indexes are prebuilt in the original mode")

	restore.OutputOptions.IndexAfter = false
	restore.indexCatalog = idx.NewIndexCatalog()
	restore.indexCatalog.AddIndex("db", "coll", &idx.IndexDocument{
		Key: bson.D{{"_id", int32(1)}}, Options: bson.M{"name": "_id_", "unique": true},
	})
	hasUnique, err = restore.restoreUniqueIndexes("db", "coll")
	require.NoError(err, "no server call is needed when only the _id index exists")
	require.False(hasUnique, "the _id index does not trigger the pre-data phase")
}
