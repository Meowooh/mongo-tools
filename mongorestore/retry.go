// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongorestore

import (
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	eloqOutOfMemoryCode = 146
	eloqOutOfMemoryName = "ExceededMemoryLimit"
	eloqOutOfMemoryTx   = "TxError[22]"
)

func isEloqOutOfMemoryError(err error) bool {
	commandErr, ok := err.(mongo.CommandError)
	return ok &&
		commandErr.Code == eloqOutOfMemoryCode &&
		commandErr.Name == eloqOutOfMemoryName &&
		strings.Contains(commandErr.Message, eloqOutOfMemoryTx)
}
