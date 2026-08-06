package dumprestore

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mongodb/mongo-tools/mongorestore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mopt "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestIndexRoundTrip checks that every kind of index survives a dump and restore
// with its full specification intact, whether the restore reads the dump
// directory or the collection's bson file directly.
func (s *DumpRestoreSuite) TestIndexRoundTrip() {
	s.Run("restore from the dump directory", s.testIndexRoundTripFromDumpDir)
	s.Run("restore from the bson file", s.testIndexRoundTripFromBSONFile)
}

func (s *DumpRestoreSuite) testIndexRoundTripFromDumpDir() {
	testDB := s.database("indexes_dump_dir")
	coll := s.createIndexFixture(testDB)
	specsBefore := s.indexSpecs(coll)

	s.withBSONMongodump(func(dir string) {
		s.dropCollection(coll)

		result := s.runRestore(dir)
		s.Require().NoError(result.Err, "can restore from the dump directory")
	}, "--db", testDB.Name())

	s.assertIndexFixtureRestored(coll, specsBefore)
}

func (s *DumpRestoreSuite) testIndexRoundTripFromBSONFile() {
	testDB := s.database("indexes_bson_file")
	coll := s.createIndexFixture(testDB)
	specsBefore := s.indexSpecs(coll)

	s.withBSONMongodump(func(dir string) {
		s.dropCollection(coll)

		result := s.runRestore(filepath.Join(dir, testDB.Name(), coll.Name()+".bson"))
		s.Require().NoError(result.Err, "can restore from the bson file")
	}, "--db", testDB.Name())

	s.assertIndexFixtureRestored(coll, specsBefore)
}

const (
	indexFixtureDocCount = 15

	// The seven indexes the fixture creates, plus the _id index.
	indexFixtureIndexCount = 8
)

// createIndexFixture creates one collection carrying an index of every kind
// worth round-tripping: simple, sparse and unique, compound, compound with
// int64 key values, multikey (via array data), text with a non-default
// language, and 2dsphere.
func (s *DumpRestoreSuite) createIndexFixture(testDB *mongo.Database) *mongo.Collection {
	ctx := s.Context()
	coll := testDB.Collection("coll")

	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{"a", 1}}},
		{Keys: bson.D{{"b", 1}}, Options: mopt.Index().SetSparse(true).SetUnique(true)},
		{Keys: bson.D{{"a", 1}, {"b", -1}}},
		{Keys: bson.D{{"b", int64(1)}, {"a", int64(1)}}},
		{Keys: bson.D{{"listField", 1}}},
		{Keys: bson.D{{"textField", "text"}}, Options: mopt.Index().SetDefaultLanguage("spanish")},
		{Keys: bson.D{{"geoField", "2dsphere"}}},
	})
	s.Require().NoError(err, "can create the fixture indexes")

	docs := make([]any, 0, indexFixtureDocCount)
	for i := range 5 {
		docs = append(
			docs,
			bson.D{{"a", i}, {"b", i + 1}, {"listField", bson.A{i, i + 1}}},
			bson.D{{"textField", "hola " + strconv.Itoa(i)}},
			bson.D{{"geoField", bson.D{
				{"type", "Point"},
				{"coordinates", bson.A{i, i + 1}},
			}}},
		)
	}
	_, err = coll.InsertMany(ctx, docs)
	s.Require().NoError(err, "can insert the fixture documents")

	s.Require().EqualValues(
		indexFixtureDocCount,
		s.docCount(coll),
		"the fixture inserted every document",
	)
	specs := s.indexSpecs(coll)
	s.Require().Len(
		specs,
		indexFixtureIndexCount,
		"the fixture created every index, so comparing specs is not vacuous",
	)
	s.requireDistinctiveIndexPropertiesPresent(specs)

	return coll
}

// requireDistinctiveIndexPropertiesPresent checks that the properties this
// fixture exists to exercise really made it into the server's specs. Without
// this, a server that normalized int64 index keys to double, or that stopped
// reporting the text language, would turn the before/after spec comparison into
// a comparison of two identically uninteresting documents.
func (s *DumpRestoreSuite) requireDistinctiveIndexPropertiesPresent(specs []bson.D) {
	specsByName := map[string]bson.D{}
	for _, spec := range specs {
		specsByName[fmt.Sprint(optionValue(spec, "name"))] = spec
	}

	compoundKey, ok := optionValue(specsByName["b_1_a_1"], "key").(bson.D)
	s.Require().True(ok, "the int64-keyed compound index reports a key document")
	for _, elem := range compoundKey {
		s.Require().IsType(
			int64(0),
			elem.Value,
			"the compound index key %#q keeps its int64 type",
			elem.Key,
		)
	}

	s.Require().Equal(
		"spanish",
		optionValue(specsByName["textField_text"], "default_language"),
		"the text index really uses a non-default language",
	)
}

// assertIndexFixtureRestored compares the whole spec of every index against
// what the server reported before the dump. Both sides come from the same server
// through the same call, so the fields arrive in the same order and the specs
// can be compared as documents.
func (s *DumpRestoreSuite) assertIndexFixtureRestored(
	coll *mongo.Collection,
	specsBefore []bson.D,
) {
	s.Assert().EqualValues(indexFixtureDocCount, s.docCount(coll), "the documents are restored")
	s.Assert().ElementsMatch(
		specsBefore,
		s.indexSpecs(coll),
		"every index is restored with an identical spec",
	)
}

// TestRestoreLegacySystemIndexes restores a dump in the pre-2.6 layout, where
// index specs live in a system.indexes.bson file instead of a per-collection
// metadata file. mongorestore falls back to that file when a database directory
// has no metadata, and converts the legacy v1 specs it finds there.
func (s *DumpRestoreSuite) TestRestoreLegacySystemIndexes() {
	const collName = "foo"

	testDB := s.database("legacy_system_indexes")
	_, dbDir := s.newDumpDir(testDB.Name())

	s.writeBSONFile(
		filepath.Join(dbDir, collName+".bson"),
		bson.D{{"_id", 1}, {"a", 2.0}},
	)
	// The namespaces deliberately name a different database than the restore
	// target, because mongorestore has to take the target from --db rather than
	// from the spec.
	s.writeBSONFile(
		filepath.Join(dbDir, "system.indexes.bson"),
		bson.D{
			{"ns", "test." + collName},
			{"key", bson.D{{"_id", 1}}},
			{"name", "_id_"},
			{"v", 1},
		},
		bson.D{
			{"ns", "test." + collName},
			{"key", bson.D{{"a", 1.0}}},
			{"name", "a_1"},
			{"v", 1},
		},
	)

	result := s.runRestore(
		mongorestore.DBOption, testDB.Name(),
		mongorestore.DirectoryOption, dbDir,
	)
	s.Require().NoError(result.Err, "can restore a dump that uses system.indexes")

	coll := testDB.Collection(collName)
	s.Assert().EqualValues(1, s.docCount(coll), "the document is restored")
	s.Assert().ElementsMatch(
		[]string{"_id_", "a_1"},
		s.indexNames(coll),
		"both legacy index specs are created",
	)

	for _, spec := range s.indexSpecs(coll) {
		s.Assert().EqualValues(
			2,
			optionValue(spec, "v"),
			"the legacy v1 index %#q is converted to the current version",
			optionValue(spec, "name"),
		)
	}
}

// TestIndexVersionRoundTrip checks how an index's version survives a restore:
// --keepIndexVersion carries the dumped version through, and without it the
// server applies its own default.
func (s *DumpRestoreSuite) TestIndexVersionRoundTrip() {
	s.Run("keepIndexVersion preserves the index version", s.testKeepIndexVersion)
	s.Run("the server default version is used otherwise", s.testDefaultIndexVersion)
}

func (s *DumpRestoreSuite) testKeepIndexVersion() {
	testDB := s.database("keep_index_version")
	s.createIDIndexVersionFixture(testDB)

	versionsBefore := s.idIndexVersions(testDB)
	s.Require().EqualValues(1, versionsBefore["v1coll"], "the fixture created a v1 _id index")
	s.Require().EqualValues(2, versionsBefore["v2coll"], "the fixture created a v2 _id index")

	s.withBSONMongodump(func(dir string) {
		s.dropDB(testDB)

		result := s.runRestore(mongorestore.KeepIndexVersionOption, dir)
		s.Require().NoError(result.Err, "can restore with --keepIndexVersion")
	}, "--db", testDB.Name())

	s.assertIndexVersionFixtureRestored(testDB)
	s.Assert().Equal(
		versionsBefore,
		s.idIndexVersions(testDB),
		"--keepIndexVersion restores each _id index at its original version",
	)
}

func (s *DumpRestoreSuite) testDefaultIndexVersion() {
	testDB := s.database("default_index_version")
	s.createIDIndexVersionFixture(testDB)

	s.Require().EqualValues(
		1,
		s.idIndexVersions(testDB)["v1coll"],
		"the fixture created a v1 _id index",
	)

	s.withBSONMongodump(func(dir string) {
		s.dropDB(testDB)

		result := s.runRestore(dir)
		s.Require().NoError(result.Err, "can restore without --keepIndexVersion")
	}, "--db", testDB.Name())

	s.assertIndexVersionFixtureRestored(testDB)
	s.Assert().Equal(
		map[string]int32{"v1coll": 2, "v2coll": 2},
		s.idIndexVersions(testDB),
		"without --keepIndexVersion every _id index gets the server's default version",
	)
}

// indexVersionFixtureColls are the collections the index-version fixture
// creates, each with an _id index at the given version.
var indexVersionFixtureColls = []struct {
	name    string
	version int
}{
	{"v1coll", 1},
	{"v2coll", 2},
}

// createIDIndexVersionFixture creates two collections whose _id indexes differ
// only in their index version. The version can only be set through the create
// command's idIndex argument: a v1 secondary index is silently upgraded to the
// server's current version, so there is no secondary index version to
// round-trip. The secondary index is still created, so the tests can check that
// restoring does not lose it.
func (s *DumpRestoreSuite) createIDIndexVersionFixture(testDB *mongo.Database) {
	for _, fixtureColl := range indexVersionFixtureColls {
		res := testDB.RunCommand(s.Context(), bson.D{
			{"create", fixtureColl.name},
			{"idIndex", bson.D{
				{"v", fixtureColl.version},
				{"key", bson.D{{"_id", 1}}},
				{"name", "_id_"},
			}},
		})
		s.Require().NoError(
			res.Err(),
			"can create %#q with a v%d _id index",
			fixtureColl.name,
			fixtureColl.version,
		)

		coll := testDB.Collection(fixtureColl.name)
		_, err := coll.Indexes().CreateOne(s.Context(), mongo.IndexModel{Keys: bson.D{{"a", 1}}})
		s.Require().NoError(err, "can create a secondary index on %#q", fixtureColl.name)

		_, err = coll.InsertOne(s.Context(), bson.D{{"a", 123}})
		s.Require().NoError(err, "can insert into %#q", fixtureColl.name)
	}
}

func (s *DumpRestoreSuite) assertIndexVersionFixtureRestored(testDB *mongo.Database) {
	for _, fixtureColl := range indexVersionFixtureColls {
		coll := testDB.Collection(fixtureColl.name)
		s.Assert().EqualValues(1, s.docCount(coll), "%#q keeps its document", fixtureColl.name)
		s.Assert().ElementsMatch(
			[]string{"_id_", "a_1"},
			s.indexNames(coll),
			"%#q keeps both of its indexes",
			fixtureColl.name,
		)
	}
}

func (s *DumpRestoreSuite) idIndexVersions(testDB *mongo.Database) map[string]int32 {
	versions := map[string]int32{}
	for _, fixtureColl := range indexVersionFixtureColls {
		for _, spec := range s.indexSpecs(testDB.Collection(fixtureColl.name)) {
			if optionValue(spec, "name") == "_id_" {
				version, ok := optionValue(spec, "v").(int32)
				s.Require().True(ok, "the _id index of %#q reports a version", fixtureColl.name)
				versions[fixtureColl.name] = version
			}
		}
	}

	return versions
}

// partialFilterFieldOrder is deliberately not in sorted order. Sorting is the
// most likely way for a document to come back reordered, so an ascending list
// would match its own corruption and the test would pass either way.
var partialFilterFieldOrder = []string{"a7", "a2", "a9", "a0", "a5", "a1", "a8", "a3", "a6", "a4"}

// TestOrderedPartialIndex restores an index whose partialFilterExpression has
// many fields. The expression is a document, so its field order has to survive
// the trip through the metadata file: reordering it would produce an index the
// server treats as different from the one that was dumped.
func (s *DumpRestoreSuite) TestOrderedPartialIndex() {
	const (
		collName  = "foo"
		indexName = "apfe"
	)

	testDB := s.database("ordered_partial_index")
	dir, dbDir := s.newDumpDir(testDB.Name())

	filter := bson.D{}
	for _, field := range partialFilterFieldOrder {
		filter = append(filter, bson.E{field, bson.D{{"$gt", 0}}})
	}

	metadata, err := bson.MarshalExtJSON(
		bson.D{
			{"options", bson.D{}},
			{"indexes", []bson.D{
				{
					{"v", 2},
					{"key", bson.D{{"_id", 1}}},
					{"name", "_id_"},
				},
				{
					{"v", 2},
					{"key", bson.D{{"a", 1.0}}},
					{"name", indexName},
					{"partialFilterExpression", filter},
				},
			}},
		},
		false,
		false,
	)
	s.Require().NoError(err, "can marshal the metadata fixture")

	s.Require().NoError(
		os.WriteFile(filepath.Join(dbDir, collName+".metadata.json"), metadata, 0644),
		"can write the metadata fixture",
	)
	s.writeBSONFile(filepath.Join(dbDir, collName+".bson"), bson.D{{"_id", 1}})

	result := s.runRestore(dir)
	s.Require().NoError(result.Err, "can restore an index with a partialFilterExpression")

	restored := s.indexPartialFilter(testDB.Collection(collName), indexName)

	gotFields := make([]string, 0, len(restored))
	for _, elem := range restored {
		gotFields = append(gotFields, elem.Key)
	}
	s.Assert().Equal(
		partialFilterFieldOrder,
		gotFields,
		"the partialFilterExpression keeps its field order through the restore",
	)
}

func (s *DumpRestoreSuite) indexPartialFilter(
	coll *mongo.Collection,
	indexName string,
) bson.D {
	cursor, err := coll.Indexes().List(s.Context())
	s.Require().NoError(err, "can list indexes on %#q", coll.Name())

	var indexes []struct {
		Name                    string `bson:"name"`
		PartialFilterExpression bson.D `bson:"partialFilterExpression"`
	}
	s.Require().NoError(cursor.All(s.Context(), &indexes), "can read the index specs")

	for _, index := range indexes {
		if index.Name == indexName {
			return index.PartialFilterExpression
		}
	}

	s.Require().Failf("index not found", "the index %#q exists on %#q", indexName, coll.Name())

	return nil
}

// indexSpecs returns the complete spec of every index on the collection.
// ListSpecifications omits fields such as the index version and the text index
// options, so the specs are listed directly.
func (s *DumpRestoreSuite) indexSpecs(coll *mongo.Collection) []bson.D {
	var specs []bson.D
	s.Require().NoError(
		listIndexes(s.Context(), coll, &specs),
		"can list the index specs on %#q",
		coll.Name(),
	)

	return specs
}
