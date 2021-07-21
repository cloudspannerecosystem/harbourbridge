// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cmd implements command line utility for HarbourBridge.
package cmd

import (
	"fmt"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

var (
	badDataFile = "dropped.txt"
	reportFile  = "report.txt"
	schemaFile  = "schema.txt"
	sessionFile = "session.json"

	// Set the maximum number of concurrent workers during foreign key creation.
	maxWorkers = int64(10)
)

// CommandLine provides the core processing for HarbourBridge when run as a command-line tool.
// It performs the following steps:
// 1. Run schema conversion (if dataOnly is set to false)
// 2. Create database (if schemaOnly is set to false)
// 3. Run data conversion (if schemaOnly is set to false)
// 4. Generate report
func CommandLine(driver, projectID, instanceID, dbName string, dataOnly, schemaOnly, skipForeignKeys bool, schemaSampleSize int64, sessionJSON string, ioHelper *conversion.IOStreams, outputFilePrefix string, now time.Time) error {
	var conv *internal.Conv
	var err error
	if !dataOnly {
		conv, err = conversion.SchemaConv(driver, ioHelper, schemaSampleSize)
		if err != nil {
			return err
		}
		if ioHelper.SeekableIn != nil {
			defer ioHelper.In.Close()
		}

		conversion.WriteSchemaFile(conv, now, outputFilePrefix+schemaFile, ioHelper.Out)
		conversion.WriteSessionFile(conv, outputFilePrefix+sessionFile, ioHelper.Out)
		if schemaOnly {
			conversion.Report(driver, nil, ioHelper.BytesRead, "", conv, outputFilePrefix+reportFile, ioHelper.Out)
			return nil
		}
	} else {
		conv = internal.MakeConv()
		err = conversion.ReadSessionFile(conv, sessionJSON)
		if err != nil {
			return err
		}
	}

	db, err := conversion.CreateDatabase(projectID, instanceID, dbName, conv, ioHelper.Out)
	if err != nil {
		fmt.Printf("\nCan't create database: %v\n", err)
		return fmt.Errorf("can't create database")
	}

	client, err := conversion.GetClient(db)
	if err != nil {
		fmt.Printf("\nCan't create client for db %s: %v\n", db, err)
		return fmt.Errorf("can't create Spanner client")
	}

	bw, err := conversion.DataConv(driver, ioHelper, client, conv, dataOnly)
	if err != nil {
		fmt.Printf("\nCan't finish data conversion for db %s: %v\n", db, err)
		return fmt.Errorf("can't finish data conversion")
	}
	if !skipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(projectID, instanceID, dbName, maxWorkers, conv, ioHelper.Out); err != nil {
			fmt.Printf("\nCan't perform update operation on db %s with foreign keys: %v\n", db, err)
			return fmt.Errorf("can't perform update schema with foreign keys")
		}
	}
	banner := conversion.GetBanner(now, db)
	conversion.Report(driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, outputFilePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, outputFilePrefix+badDataFile, ioHelper.Out)
	return nil
}
