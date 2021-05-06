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

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	instanceAdmin *instance.InstanceAdminClient
	databaseAdmin *database.DatabaseAdminClient
)

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx := context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID is missing")
		return noop
	}

	var err error
	databaseAdmin, err = database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}

	return func() {
		databaseAdmin.Close()
	}
}

func dropDatabase(t *testing.T, dbPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Drop the testing database.
	if err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbPath, err)
	}
}

func cleanupFiles(t *testing.T, files []string) {
	for _, file := range files {
		if _, err := os.Stat(file); err == nil {
			err = os.Remove(file)
			if err != nil {
				t.Errorf("failed to delete file: %v", file)
			}
		}
	}
}

func prepareIntegrationTest(t *testing.T) string {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	return tmpdir
}

func TestIntegration_PGDUMP_SimpleUse(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.PGDUMP, now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	dataFilepath := "test_data/pg_dump.test.out"
	filePrefix = filepath.Join(tmpdir, dbName+".")
	f, err := os.Open(dataFilepath)
	if err != nil {
		t.Fatalf("failed to open the test data file: %v", err)
	}
	err = commandLine(conversion.PGDUMP, projectID, instanceID, dbName, &conversion.IOStreams{In: f, Out: os.Stdout}, filePrefix, now, false)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	checkResults(t, dbPath)
}

func TestIntegration_PGDUMP_Command(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.PGDUMP, now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	dataFilepath := "test_data/pg_dump.test.out"
	filePrefix = filepath.Join(tmpdir, dbName+".")
	// Be aware that when testing with the command, the time `now` might be
	// different between file prefixes and the contents in the files. This
	// is because file prefixes use `now` from here (the test function) and
	// the generated time in the files uses a `now` inside the command, which
	// can be different.
	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/cloudspannerecosystem/harbourbridge -instance %s -dbname %s -prefix %s < %s", instanceID, dbName, filePrefix, dataFilepath))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	checkResults(t, dbPath)
}

func TestIntegration_POSTGRES_SimpleUse(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.POSTGRES, now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix = filepath.Join(tmpdir, dbName+".")

	err := commandLine(conversion.POSTGRES, projectID, instanceID, dbName, &conversion.IOStreams{Out: os.Stdout}, filePrefix, now, false)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	checkResults(t, dbPath)
}

func TestIntegration_POSTGRES_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.POSTGRES, now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix = filepath.Join(tmpdir, dbName+".")

	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/cloudspannerecosystem/harbourbridge -instance %s -dbname %s -prefix %s -driver %s", instanceID, dbName, filePrefix, conversion.POSTGRES))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	checkResults(t, dbPath)
}

func checkResults(t *testing.T, dbPath string) {
	// Make a query to check results.
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkBigInt(ctx, t, client)
	checkTimestamps(ctx, t, client)
	checkCoreTypes(ctx, t, client)
	checkArrays(ctx, t, client)
}

func checkBigInt(ctx context.Context, t *testing.T, client *spanner.Client) {
	var quantity int64
	iter := client.Single().Read(ctx, "cart", spanner.Key{"31ad80e3-182b-42b0-a164-b4c7ea976ce4", "OLJCESPC7Z"}, []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&quantity); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := quantity, int64(125); got != want {
		t.Fatalf("quantities are not correct: got %v, want %v", got, want)
	}
}

func checkTimestamps(ctx context.Context, t *testing.T, client *spanner.Client) {
	var ts, tsWithZone time.Time
	iter := client.Single().Read(ctx, "test", spanner.Key{4}, []string{"t", "tz"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&ts, &tsWithZone); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := ts.Format(time.RFC3339Nano), "2019-10-28T15:00:00.123457Z"; got != want {
		t.Fatalf("timestamp is not correct: got %v, want %v", got, want)
	}
	if got, want := tsWithZone.Format(time.RFC3339Nano), "2019-10-28T15:00:00.123457Z"; got != want {
		t.Fatalf("timestamp with time zone is not correct: got %v, want %v", got, want)
	}
}

func checkCoreTypes(ctx context.Context, t *testing.T, client *spanner.Client) {
	var boolVal bool
	var bytesVal []byte
	var date spanner.NullDate
	var floatVal float64
	var intVal int64
	var numericVal big.Rat
	var stringVal string
	iter := client.Single().Read(ctx, "test2", spanner.Key{1}, []string{"a", "b", "c", "d", "e", "f", "g"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&boolVal, &bytesVal, &date, &floatVal, &intVal, &numericVal, &stringVal); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := boolVal, true; got != want {
		t.Fatalf("bool value is not correct: got %v, want %v", got, want)
	}
	if got, want := string(bytesVal), "\x00\x01\x02\x03ޭ\xbe\xef"; got != want {
		t.Fatalf("bytes are not correct: got %v, want %v", got, want)
	}
	if got, want := date.String(), "2019-10-28"; got != want {
		t.Fatalf("date is not correct: got %v, want %v", got, want)
	}
	if got, want := floatVal, 99.9; got != want {
		t.Fatalf("float value is not correct: got %v, want %v", got, want)
	}
	if got, want := intVal, int64(42); got != want {
		t.Fatalf("int value is not correct: got %v, want %v", got, want)
	}
	if got, want := spanner.NumericString(&numericVal), "1234567890123456789012345678.123456789"; got != want {
		t.Fatalf("numeric value is not correct: got %v, want %v", got, want)
	}
	if got, want := stringVal, "hi"; got != want {
		t.Fatalf("string value is not correct: got %v, want %v", got, want)
	}
}

func checkArrays(ctx context.Context, t *testing.T, client *spanner.Client) {
	var ints []int64
	var strs []string
	iter := client.Single().Read(ctx, "test3", spanner.Key{1}, []string{"a", "b"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&ints, &strs); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := ints, []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("integer array is not correct: got %v, want %v", got, want)
	}
	if got, want := strs, []string{"1", "nice", "foo"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("string array is not correct: got %v, want %v", got, want)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
