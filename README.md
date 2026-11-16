MongoDB Tools
===================================

 - **bsondump** - _display BSON files in a human-readable format_
 - **mongoimport** - _Convert data from JSON, TSV or CSV and insert them into a collection_
 - **mongoexport** - _Write an existing collection to CSV or JSON format_
 - **mongodump/mongorestore** - _Dump MongoDB backups to disk in .BSON format, or restore them to a live database_
 - **mongostat** - _Monitor live MongoDB servers, replica sets, or sharded clusters_
 - **mongofiles** - _Read, write, delete, or update files in [GridFS](http://docs.mongodb.org/manual/core/gridfs/)_
 - **mongotop** - _Monitor read/write activity on a mongo server_


Report any bugs, improvements, or new feature requests at https://jira.mongodb.org/browse/TOOLS

EloqDoc / TiDoc index restoration
--------------------------------

This fork restores unique secondary indexes **before** it imports each collection's data.
EloqDoc cannot create a new unique secondary index on a non-empty collection.
Other secondary indexes remain in the final index phase.
The collection creation path still handles the `_id` index.

| Options | Unique secondary indexes | Other secondary indexes |
| --- | --- | --- |
| Default | Before data | After data |
| `--noIndexRestore` | **Before data** | Not restored |
| `--indexAfter` | After data (original behavior) | After data |
| `--indexAfter --noIndexRestore` | Not restored | Not restored |

The flag `--indexAfter` is case-sensitive.
Unlike the original tools, `--noIndexRestore` alone does **not** skip unique secondary indexes.
Use both flags to import data without restoring any secondary indexes.
Neither flag removes indexes that already exist on the target.

The tool reads index definitions from the existing dump metadata, including archive metadata.
It does not scan the data twice or infer index definitions from documents.
A BSON file without index metadata cannot supply unique index definitions.

For each collection, the tool applies an explicit `--drop` first, then creates or reuses the collection.
It creates the unique secondary indexes before it opens the collection's BSON input.
An equivalent unique index that already exists needs no new build.
A failed index creation stops that collection before data import.
The tool never automatically drops a non-empty collection to resolve a missing unique index.

If the tool restores unique indexes before data, insertion errors for that collection are fatal, including duplicate-key errors.
The restore can leave partial data after an error. It does not roll back earlier writes.
Keep other writers stopped throughout the restore, including during index creation.

`--oplogReplay` requires `--indexAfter`, even with `--noIndexRestore`.
Oplog replay can change index definitions and requires separate support for the new restore order.
Selecting `--indexAfter` does not remove EloqDoc's restriction on new unique indexes in non-empty collections.

Building Tools
---------------

We currently build the tools with Go version 1.15. Other Go versions may work but they are untested.

Using `go get` to directly build the tools will not work. To build them, it's recommended to first clone this repository:

```
git clone https://github.com/mongodb/mongo-tools
cd mongo-tools
```

Then run `./make build` to build all the tools, placing them in the `bin` directory inside the repository.

You can also build a subset of the tools using the `-pkgs` option. For example, `./make build -pkgs=mongodump,mongorestore` builds only `mongodump` and `mongorestore`.

To use the build/test scripts in this repository, you **_must_** set GOROOT to your Go root directory. This may depend on how you installed Go.

```
export GOROOT=/usr/local/go
```

Updating Dependencies
---------------
Starting with version 100.3.1, the tools use `go mod` to manage dependencies. All dependencies are listed in the `go.mod` file and are directly vendored in the `vendor` directory.

In order to make changes to dependencies, you first need to change the `go.mod` file. You can manually edit that file to add/update/remove entries, or you can run the following in the repository directory:

```
go mod edit -require=<package>@<version>  # for adding or updating a dependency
go mod edit -droprequire=<package>        # for removing a dependency
```

Then run `go mod vendor -v` to reconstruct the `vendor` directory to match the changed `go.mod` file.

Optionally, run `go mod tidy -v` to ensure that the `go.mod` file matches the `mongo-tools` source code.

Contributing
---------------
See our [Contributor's Guide](CONTRIBUTING.md).

Documentation
---------------
See the MongoDB packages [documentation](https://docs.mongodb.org/database-tools/).

For documentation on older versions of the MongoDB, reference that version of the [MongoDB Server Manual](docs.mongodb.com/manual):

- [MongoDB 4.2 Tools](https://docs.mongodb.org/v4.2/reference/program)
- [MongoDB 4.0 Tools](https://docs.mongodb.org/v4.0/reference/program)
- [MongoDB 3.6 Tools](https://docs.mongodb.org/v3.6/reference/program)

Adding New Platforms Support
---------------
See our [Adding New Platform Support Guide](PLATFORMSUPPORT.md).

Vendoring the Change into Server Repo
---------------
See our [Vendor the Change into Server Repo](SERVERVENDORING.md).
