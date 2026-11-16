// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"fmt"
	"sort"

	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func isIDIndex(index *idx.IndexDocument) bool {
	return len(index.Key) == 1 && index.Key[0].Key == "_id"
}

// isUniqueIndex also accepts the non-boolean values found in legacy metadata,
// using BSON truthiness rather than treating every non-boolean as false.
func isUniqueIndex(index *idx.IndexDocument) bool {
	switch value := index.Options["unique"].(type) {
	case nil, primitive.Undefined:
		return false
	case bool:
		return value
	case int:
		return value != 0
	case int32:
		return value != 0
	case int64:
		return value != 0
	case float64:
		return value != 0
	case primitive.Decimal128:
		number, _, err := value.BigInt()
		return err != nil || number.Sign() != 0
	default:
		return true
	}
}

// prepareIndexesForNamespace gives both restore phases the same index specs.
// In the default mode, work on copies so normalization and createIndexes do not
// mutate the catalog between the pre-data and post-data phases.
func (restore *MongoRestore) prepareIndexesForNamespace(dbName, collectionName string) ([]*idx.IndexDocument, error) {
	namespace := dbName + "." + collectionName
	var indexes []*idx.IndexDocument
	for _, index := range restore.indexCatalog.GetIndexes(dbName, collectionName) {
		if isIDIndex(index) {
			// The collection creation path already handles the _id index.
			continue
		}
		if !restore.OutputOptions.IndexAfter {
			if restore.OutputOptions.NoIndexRestore && !isUniqueIndex(index) {
				continue
			}
			// Normalization changes key values and top-level options only.
			// Keep nested documents intact, including their BSON field order.
			copyIndex := *index
			copyIndex.Key = append(bson.D(nil), index.Key...)
			copyIndex.Options = make(bson.M, len(index.Options))
			for key, value := range index.Options {
				copyIndex.Options[key] = value
			}
			index = &copyIndex
		}
		indexes = append(indexes, index)
	}

	if !restore.OutputOptions.IndexAfter {
		// The catalog is a map. Use a stable order in both phases, including
		// when legacy conversion chooses between duplicate non-unique specs.
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Options["name"].(string) < indexes[j].Options["name"].(string)
		})
	}
	if restore.OutputOptions.ConvertLegacyIndexes {
		if !restore.OutputOptions.IndexAfter {
			for _, index := range indexes {
				bsonutil.ConvertLegacyIndexKeys(index.Key, namespace)
			}
			// Legacy conversion deduplicates by key alone. Do not let it
			// discard a unique constraint or its partial/sparse definition.
			if err := checkUniqueIndexKeyCollisions(indexes, namespace); err != nil {
				return nil, err
			}
		}
		indexes = restore.convertLegacyIndexes(indexes, namespace)
	}
	if restore.OutputOptions.FixDottedHashedIndexes {
		fixDottedHashedIndexes(indexes)
		if !restore.OutputOptions.IndexAfter {
			if err := checkUniqueIndexKeyCollisions(indexes, namespace); err != nil {
				return nil, err
			}
		}
	}
	return indexes, nil
}

func checkUniqueIndexKeyCollisions(indexes []*idx.IndexDocument, namespace string) error {
	for i, index := range indexes {
		for _, previous := range indexes[:i] {
			if (isUniqueIndex(index) || isUniqueIndex(previous)) &&
				bsonutil.IsIndexKeysEqual(index.Key, previous.Key) {
				return fmt.Errorf("index conversion for %s gives indexes %v and %v the same key pattern with a unique constraint. Resolve the conflicting index definitions before restoring",
					namespace, previous.Options["name"], index.Options["name"])
			}
		}
	}
	return nil
}

// restoreUniqueIndexes runs after collection creation (and any explicit drop),
// but before opening its BSON input. NoIndexRestore deliberately does not skip
// this phase: only IndexAfter restores the original index ordering and semantics.
// The result reports whether inserts must enforce the prebuilt constraints as
// fatal errors instead of continuing through duplicate-key failures.
func (restore *MongoRestore) restoreUniqueIndexes(dbName, collectionName string) (bool, error) {
	if restore.OutputOptions.IndexAfter {
		return false, nil
	}
	indexes, err := restore.prepareIndexesForNamespace(dbName, collectionName)
	if err != nil {
		return false, err
	}
	var uniqueIndexes []*idx.IndexDocument
	for _, index := range indexes {
		if isUniqueIndex(index) {
			uniqueIndexes = append(uniqueIndexes, index)
		}
	}
	if len(uniqueIndexes) == 0 {
		return false, nil
	}
	log.Logvf(log.Always, "restoring unique indexes for collection %s.%s before data", dbName, collectionName)
	if err := restore.CreateIndexes(dbName, collectionName, uniqueIndexes); err != nil {
		return false, fmt.Errorf("error creating unique indexes for %s.%s before restoring data: %w", dbName, collectionName, err)
	}
	return true, nil
}
