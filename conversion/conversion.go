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

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
// 			public constants first
// 			key public type definitions next (although often it makes sense to put them next to public functions that use them)
// 			then public functions (and relevant type definitions)
// 			and helper functions and other non-public definitions last (generally in order of importance)

package conversion

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/cloudspannerecosystem/harbourbridge/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

const (
	// PGDUMP is the driver name for pg_dump.
	PGDUMP string = "pg_dump"
	// POSTGRES is the driver name for PostgreSQL.
	POSTGRES string = "postgres"
	// MYSQLDUMP is the driver name for mysqldump.
	MYSQLDUMP string = "mysqldump"
	// MYSQL is the driver name for MySQL.
	MYSQL string = "mysql"
	// DYNAMODB is the driver name for AWS DynamoDB.
	// This is an experimental driver; implementation in progress.
	DYNAMODB string = "dynamodb"
)

func SchemaConv(driver string, ioHelper *IOStreams, schemaSampleSize int64) (*internal.Conv, error) {
	switch driver {
	case POSTGRES, MYSQL:
		return schemaFromSQL(driver)
	case PGDUMP, MYSQLDUMP:
		return schemaFromDump(driver, ioHelper)
	case DYNAMODB:
		return schemaFromDynamoDB(schemaSampleSize)
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", driver)
	}
}

// Check if schema contains interleaved tables.
func checkInterleaved(conv *internal.Conv) bool {
	for _, table := range conv.SpSchema {
		if table.Parent != "" {
			return true
		}
	}
	return false
}

func DataConv(driver string, ioHelper *IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*spanner.BatchWriter, error) {
	config := spanner.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: 40,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch driver {
	case POSTGRES, MYSQL:
		return dataFromSQL(driver, config, client, conv)
	case PGDUMP, MYSQLDUMP:
		if checkInterleaved(conv) {
			return nil, fmt.Errorf("HarbourBridge does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql.")
		}
		return dataFromDump(driver, config, ioHelper, client, conv, dataOnly)
	case DYNAMODB:
		return dataFromDynamoDB(config, client, conv)
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", driver)
	}
}

func driverConfig(driver string) (string, error) {
	switch driver {
	case POSTGRES:
		return pgDriverConfig()
	case MYSQL:
		return mysqlDriverConfig()
	default:
		return "", fmt.Errorf("Driver %s not supported", driver)
	}
}

func pgDriverConfig() (string, error) {
	server := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	dbname := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n")
		return "", fmt.Errorf("Could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = getPassword()
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", server, port, user, password, dbname), nil
}

func mysqlDriverConfig() (string, error) {
	server := os.Getenv("MYSQLHOST")
	port := os.Getenv("MYSQLPORT")
	user := os.Getenv("MYSQLUSER")
	dbname := os.Getenv("MYSQLDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n")
		return "", fmt.Errorf("Could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		password = getPassword()
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbname), nil
}

func schemaFromSQL(driver string) (*internal.Conv, error) {
	driverConfig, err := driverConfig(driver)
	if err != nil {
		return nil, err
	}
	sourceDB, err := sql.Open(driver, driverConfig)
	if err != nil {
		return nil, err
	}
	conv := internal.MakeConv()
	err = ProcessInfoSchema(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

func dataFromSQL(driver string, config spanner.BatchWriterConfig, client *sp.Client, conv *internal.Conv) (*spanner.BatchWriter, error) {
	// TODO: Refactor to avoid redundant calls to driverConfig and
	// Open in schemaFromSQL and dataFromSQL. Also refactor to
	// share code with dataFromPgDump. Use single transaction for
	// reading schema and data from source db to get consistent
	// dump.
	driverConfig, err := driverConfig(driver)
	if err != nil {
		return nil, err
	}
	sourceDB, err := sql.Open(driver, driverConfig)
	if err != nil {
		return nil, err
	}
	err = SetRowStats(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose())
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})
	err = ProcessSQLData(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return writer, nil
}

func schemaFromDynamoDB(sampleSize int64) (*internal.Conv, error) {
	conv := internal.MakeConv()
	mySession := session.Must(session.NewSession())
	client := dydb.New(mySession)
	err := dynamodb.ProcessSchema(conv, client, []string{}, sampleSize)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

func dataFromDynamoDB(config spanner.BatchWriterConfig, client *sp.Client, conv *internal.Conv) (*spanner.BatchWriter, error) {
	mySession := session.Must(session.NewSession())
	dyclient := dydb.New(mySession)

	dynamodb.SetRowStats(conv, dyclient)
	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose())

	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})

	err := dynamodb.ProcessData(conv, dyclient)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return writer, nil
}

type IOStreams struct {
	In, SeekableIn, Out *os.File
	BytesRead           int64
}

func schemaFromDump(driver string, ioHelper *IOStreams) (*internal.Conv, error) {
	f, n, err := getSeekable(ioHelper.In)
	if err != nil {
		printSeekError(driver, err, ioHelper.Out)
		return nil, fmt.Errorf("can't get seekable input file")
	}
	ioHelper.SeekableIn = f
	ioHelper.BytesRead = n
	conv := internal.MakeConv()
	p := internal.NewProgress(n, "Generating schema", internal.Verbose())
	r := internal.NewReader(bufio.NewReader(f), p)
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	err = ProcessDump(driver, conv, r)
	if err != nil {
		fmt.Fprintf(ioHelper.Out, "Failed to parse the data file: %v", err)
		return nil, fmt.Errorf("failed to parse the data file")
	}
	p.Done()
	return conv, nil
}

func dataFromDump(driver string, config spanner.BatchWriterConfig, ioHelper *IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*spanner.BatchWriter, error) {
	// TODO: refactor of the way we handle getSeekable
	// to avoid the code duplication here
	if !dataOnly {
		_, err := ioHelper.SeekableIn.Seek(0, 0)
		if err != nil {
			fmt.Printf("\nCan't seek to start of file (preparation for second pass): %v\n", err)
			return nil, fmt.Errorf("can't seek to start of file")
		}
	} else {
		// Note: input file is kept seekable to plan for future
		// changes in showing progress for data migration.
		f, n, err := getSeekable(ioHelper.In)
		if err != nil {
			printSeekError(driver, err, ioHelper.Out)
			return nil, fmt.Errorf("can't get seekable input file")
		}
		ioHelper.SeekableIn = f
		ioHelper.BytesRead = n
	}
	totalRows := conv.Rows()

	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose())
	r := internal.NewReader(bufio.NewReader(ioHelper.SeekableIn), nil)
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode() // Process data in dump; schema is unchanged.
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})
	ProcessDump(driver, conv, r)
	writer.Flush()
	p.Done()

	return writer, nil
}

// Report generates a report of schema and data conversion.
func Report(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, out *os.File) {
	f, err := os.Create(reportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)

	summary := internal.GenerateReport(driver, conv, w, badWrites, true, true)
	w.Flush()
	var isDump bool
	if strings.Contains(driver, "dump") {
		isDump = true
	}
	if isDump {
		fmt.Fprintf(out, "Processed %d bytes of %s data (%d statements, %d rows of data, %d errors, %d unexpected conditions).\n",
			BytesRead, driver, conv.Statements(), conv.Rows(), conv.StatementErrors(), conv.Unexpecteds())
	} else {
		fmt.Fprintf(out, "Processed source database via %s driver (%d rows of data, %d unexpected conditions).\n",
			driver, conv.Rows(), conv.Unexpecteds())
	}
	// We've already written summary to f (as part of GenerateReport).
	// In the case where f is stdout, don't write a duplicate copy.
	if f != out {
		fmt.Fprint(out, summary)
		fmt.Fprintf(out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

// getSeekable returns a seekable file (with same content as f) and the size of the content (in bytes).
func getSeekable(f *os.File) (*os.File, int64, error) {
	_, err := f.Seek(0, 0)
	if err == nil { // Stdin is seekable, let's just use that. This happens when you run 'cmd < file'.
		n, err := getSize(f)
		return f, n, err
	}
	internal.VerbosePrintln("Creating a tmp file with a copy of stdin because stdin is not seekable.")

	// Create file in os.TempDir. Its not clear this is a good idea e.g. if the
	// pg_dump/mysqldump output is large (tens of GBs) and os.TempDir points to a directory
	// (such as /tmp) that's configured with a small amount of disk space.
	// To workaround such limits on Unix, set $TMPDIR to a directory with lots
	// of disk space.
	fcopy, err := ioutil.TempFile("", "harbourbridge.data")
	if err != nil {
		return nil, 0, err
	}
	syscall.Unlink(fcopy.Name()) // File will be deleted when this process exits.
	_, err = io.Copy(fcopy, f)
	if err != nil {
		return nil, 0, fmt.Errorf("can't write stdin to tmp file: %w", err)
	}
	_, err = fcopy.Seek(0, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("can't reset file offset: %w", err)
	}
	n, err := getSize(fcopy)
	return fcopy, n, nil
}

func GetDatabaseAdminClient() (*database.DatabaseAdminClient, error) {
	ctx := context.Background()
	endpoint, err := GetEndpoint()
	if err != nil {
		return nil, err
	}
	if endpoint != "" {
		opt := []option.ClientOption{option.WithEndpoint(endpoint)}
		return database.NewDatabaseAdminClient(ctx, opt...)
	}
	return database.NewDatabaseAdminClient(ctx)
}

// CreateDatabase returns a newly create Spanner DB.
// It automatically determines an appropriate project, selects a
// Spanner instance to use, generates a new Spanner DB name,
// and call into the Spanner admin interface to create the new DB.
func CreateDatabase(project, instance, dbName string, conv *internal.Conv, out *os.File) (string, error) {
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... ", dbName, instance)
	ctx := context.Background()
	adminClient, err := GetDatabaseAdminClient()
	if err != nil {
		return "", fmt.Errorf("can't create admin client: %w", analyzeError(err, project, instance))
	}
	defer adminClient.Close()
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	schema := conv.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false})
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
		ExtraStatements: schema,
	})
	if err != nil {
		return "", fmt.Errorf("can't build CreateDatabaseRequest: %w", analyzeError(err, project, instance))
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("createDatabase call failed: %w", analyzeError(err, project, instance))
	}
	fmt.Fprintf(out, "done.\n")
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName), nil
}

func UpdateDDL(project, instance, dbName string, conv *internal.Conv, out *os.File) error {
	fmt.Fprintf(out, "updating existing database %s in instance %s with default permissions ... ", dbName, instance)
	ctx := context.Background()
	adminClient, err := GetDatabaseAdminClient()
	if err != nil {
		return fmt.Errorf("can't create admin client: %w\n", analyzeError(err, project, instance))
	}
	defer adminClient.Close()
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	schema := conv.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false})
	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName),
		Statements: schema,
	})
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", analyzeError(err, project, instance))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("updateDatabaseDdl call failed: %w", analyzeError(err, project, instance))
	}
	fmt.Fprintf(out, "done.\n")
	return nil
}

// UpdateDDLForeignKeys updates the Spanner database with foreign key
// constraints using ALTER TABLE statements.
func UpdateDDLForeignKeys(project, instance, dbName string, conv *internal.Conv, out *os.File) error {
	ctx := context.Background()
	adminClient, err := GetDatabaseAdminClient()
	if err != nil {
		return fmt.Errorf("can't create admin client: %w\n", analyzeError(err, project, instance))
	}
	defer adminClient.Close()
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	fkStmts := conv.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: false, ForeignKeys: true})
	if len(fkStmts) == 0 {
		return nil
	}
	msg := fmt.Sprintf("Updating schema of database %s in instance %s with foreign key constraints ...", dbName, instance)
	p := internal.NewProgress(int64(len(fkStmts)), msg, internal.Verbose())
	for i, fkStmt := range fkStmts {
		// TODO: Improve performance of the foreign key constraint updates.
		// For example, issue all of the update ops first before waiting for them to complete
		// so that that can execute in parallel. We could also print out ids of the
		// long-running operations.
		op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName),
			Statements: []string{fkStmt},
		})
		if err != nil {
			fmt.Printf("Can't add foreign key with statement %s: %s\n", fkStmt, err)
			conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
			continue
		}
		if err := op.Wait(ctx); err != nil {
			fmt.Printf("Can't add foreign key with statement %s: %s\n", fkStmt, err)
			conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
		}
		internal.VerbosePrintln("Updated schema with statement: " + fkStmt)
		p.MaybeReport(int64(i + 1))
	}
	p.Done()
	return nil
}

// GetProject returns the cloud project we should use for accessing Spanner.
// Use environment variable GCLOUD_PROJECT if it is set.
// Otherwise, use the default project returned from gcloud.
func GetProject() (string, error) {
	project := os.Getenv("GCLOUD_PROJECT")
	if project != "" {
		return project, nil
	}
	cmd := exec.Command("gcloud", "config", "list", "--format", "value(core.project)")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("call to gcloud to get project failed: %w", err)
	}
	project = strings.TrimSpace(string(out))
	return project, nil
}

// GetInstance returns the Spanner instance we should use for creating DBs.
// If the user specified instance (via flag 'instance') then use that.
// Otherwise try to deduce the instance using gcloud.
func GetInstance(project string, out *os.File) (string, error) {
	l, err := getInstances(project)
	if err != nil {
		return "", err
	}
	if len(l) == 0 {
		fmt.Fprintf(out, "Could not find any Spanner instances for project %s\n", project)
		return "", fmt.Errorf("no Spanner instances for %s", project)
	}
	// Note: we could ask for user input to select/confirm which Spanner
	// instance to use, but that interacts poorly with piping pg_dump/mysqldump data
	// to the tool via stdin.
	if len(l) == 1 {
		fmt.Fprintf(out, "Using only available Spanner instance: %s\n", l[0])
		return l[0], nil
	}
	fmt.Fprintf(out, "Available Spanner instances:\n")
	for i, x := range l {
		fmt.Fprintf(out, " %d) %s\n", i+1, x)
	}
	fmt.Fprintf(out, "Please pick one of the available instances and set the flag '--instance'\n\n")
	return "", fmt.Errorf("auto-selection of instance failed: project %s has more than one Spanner instance. "+
		"Please use the flag '--instance' to select an instance", project)
}

func GetInstanceAdminClient() (*instance.InstanceAdminClient, error) {
	ctx := context.Background()
	endpoint, err := GetEndpoint()
	if err != nil {
		return nil, err
	}
	if endpoint != "" {
		opt := []option.ClientOption{option.WithEndpoint(endpoint)}
		return instance.NewInstanceAdminClient(ctx, opt...)
	}
	return instance.NewInstanceAdminClient(ctx)
}

func getInstances(project string) ([]string, error) {
	instanceClient, err := GetInstanceAdminClient()
	if err != nil {
		return nil, analyzeError(err, project, "")
	}
	ctx := context.Background()
	it := instanceClient.ListInstances(ctx, &instancepb.ListInstancesRequest{Parent: fmt.Sprintf("projects/%s", project)})
	var l []string
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, analyzeError(err, project, "")
		}
		l = append(l, strings.TrimPrefix(resp.Name, fmt.Sprintf("projects/%s/instances/", project)))
	}
	return l, nil
}

// WriteSchemaFile writes DDL statements in a file. It includes CREATE TABLE
// statements and ALTER TABLE statements to add foreign keys.
func WriteSchemaFile(conv *internal.Conv, now time.Time, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create schema file %s: %v\n", name, err)
		return
	}
	// The schema file we write out includes comments, includes foreign keys
	// and doesn't add backticks around table and column names. This file is
	// intended for explanatory and documentation purposes, and is not strictly
	// legal Cloud Spanner DDL (Cloud Spanner doesn't currently support comments).
	// Change 'Comments' to false and 'ProtectIds' to true to write out a
	// schema file that is legal Cloud Spanner DDL.
	ddl := conv.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true})
	if len(ddl) == 0 {
		ddl = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(ddl, ";\n\n"),
		"\n",
	}
	if _, err := f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out schema file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote schema to file '%s'.\n", name)
}

// WriteSessionFile writes conv struct to a file in JSON format.
func WriteSessionFile(conv *internal.Conv, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create session file %s: %v\n", name, err)
		return
	}
	// Session file will basically contain 'conv' struct in JSON format.
	// It contains all the information for schema and data conversion state.
	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		fmt.Fprintf(out, "Can't encode session state to JSON: %v\n", err)
		return
	}
	if _, err := f.Write(convJSON); err != nil {
		fmt.Fprintf(out, "Can't write out session file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote session to file '%s'.\n", name)
}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := ioutil.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}

// WriteBadData prints summary stats about bad rows and writes detailed info
// to file 'name'.
func WriteBadData(bw *spanner.BatchWriter, conv *internal.Conv, banner, name string, out *os.File) {
	badConversions := conv.BadRows()
	badWrites := sum(bw.DroppedRowsByTable())
	if badConversions == 0 && badWrites == 0 {
		os.Remove(name) // Cleanup bad-data file from previous run.
		return
	}
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
		return
	}
	f.WriteString(banner)
	maxRows := 100
	if badConversions > 0 {
		l := conv.SampleBadRows(maxRows)
		if int64(len(l)) < badConversions {
			f.WriteString("A sample of rows that generated conversion errors:\n")
		} else {
			f.WriteString("Rows that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badWrites > 0 {
		l := bw.SampleBadRows(maxRows)
		if int64(len(l)) < badWrites {
			f.WriteString("A sample of rows that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Rows that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	fmt.Fprintf(out, "See file '%s' for details of bad rows\n", name)
}

// GetDatabaseName generates database name with driver_date prefix.
func GetDatabaseName(driver string, now time.Time) (string, error) {
	return generateName(fmt.Sprintf("%s_%s", driver, now.Format("2006-01-02")))
}

func getPassword() string {
	fmt.Print("Enter Password: ")
	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Println("\nCoudln't read password")
		return ""
	}
	fmt.Printf("\n")
	return strings.TrimSpace(string(bytePassword))
}

// analyzeError inspects an error returned from Cloud Spanner and adds information
// about potential root causes e.g. authentication issues.
func analyzeError(err error, project, instance string) error {
	e := strings.ToLower(err.Error())
	if containsAny(e, []string{"unauthenticated", "cannot fetch token", "default credentials"}) {
		return fmt.Errorf("%w.\n"+`
Possible cause: credentials are mis-configured. Do you need to run

  gcloud auth application-default login

or configure environment variable GOOGLE_APPLICATION_CREDENTIALS.
See https://cloud.google.com/docs/authentication/getting-started.
`, err)
	}
	if containsAny(e, []string{"instance not found"}) && instance != "" {
		return fmt.Errorf("%w.\n"+`
Possible cause: Spanner instance specified via instance option does not exist.
Please check that '%s' is correct and that it is a valid Spanner
instance for project %s.
`, err, instance, project)
	}
	return err
}

// PrintPermissionsWarning prints permission warning.
func PrintPermissionsWarning(driver string, out *os.File) {
	fmt.Fprintf(out,
		`
WARNING: Please check that permissions for this Spanner instance are
appropriate. Spanner manages access control at the database level, and the
database created by HarbourBridge will inherit default permissions from this
instance. All data written to Spanner will be visible to anyone who can
access the created database. Note that `+driver+` table-level and row-level
ACLs are dropped during conversion since they are not supported by Spanner.

`)
}

func printSeekError(driver string, err error, out *os.File) {
	fmt.Fprintf(out, "\nCan't get seekable input file: %v\n", err)
	fmt.Fprintf(out, "Likely cause: not enough space in %s.\n", os.TempDir())
	fmt.Fprintf(out, "Try writing "+driver+" output to a file first i.e.\n")
	fmt.Fprintf(out, " "+driver+" > tmpfile\n")
	fmt.Fprintf(out, "  harbourbridge < tmpfile\n")
}

func containsAny(s string, l []string) bool {
	for _, a := range l {
		if strings.Contains(s, a) {
			return true
		}
	}
	return false
}

func generateName(prefix string) (string, error) {
	b := make([]byte, 4)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("error generating name: %w", err)

	}
	return fmt.Sprintf("%s_%x-%x", prefix, b[0:2], b[2:4]), nil
}

// GetEndpoint returns the spanner endpoint we should use to connect to the service.
// Use the environment variable SPANNER_ENDPOINT if set.
// Othewise use the default endpoint.
func GetEndpoint() (string, error) {
	endpoint := os.Getenv("SPANNER_ENDPOINT")
	if endpoint != "" {
		return endpoint, nil
	}
	return "", nil
}

// GetClient returns new spanner client.
func GetClient(db string) (*sp.Client, error) {
	ctx := context.Background()
	if endpoint, _ := GetEndpoint(); endpoint != "" {
		opt := []option.ClientOption{option.WithEndpoint(endpoint)}
		return sp.NewClient(ctx, db, opt...)
	}
	return sp.NewClient(ctx, db)
}

func getSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("can't stat file: %w", err)
	}
	return info.Size(), nil
}

// SetupLogFile configures the file used for logs.
// By default we just drop logs on the floor. To enable them (e.g. to debug
// Cloud Spanner client library issues), set logfile to a non-empty filename.
// Note: this tool itself doesn't generate logs, but some of the libraries it
// uses do. If we don't set the log file, we see a number of unhelpful and
// unactionable logs spamming stdout, which is annoying and confusing.
func SetupLogFile() (*os.File, error) {
	// To enable debug logs, set logfile to a non-empty filename.
	logfile := ""
	if logfile == "" {
		log.SetOutput(ioutil.Discard)
		return nil, nil
	}
	f, err := os.Create(logfile)
	if err != nil {
		return nil, err
	}
	log.SetOutput(f)
	return f, nil
}

// Close closes file.
func Close(f *os.File) {
	if f != nil {
		f.Close()
	}
}

func sum(m map[string]int64) int64 {
	n := int64(0)
	for _, c := range m {
		n += c
	}
	return n
}

// GetBanner prints banner message after command line process is finished.
func GetBanner(now time.Time, db string) string {
	return fmt.Sprintf("Generated at %s for db %s\n\n", now.Format("2006-01-02 15:04:05"), db)
}

// ProcessDump invokes process dump function from a sql package based on driver selected.
func ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error {
	switch driver {
	case MYSQLDUMP:
		return mysql.ProcessMySQLDump(conv, r)
	case PGDUMP:
		return postgres.ProcessPgDump(conv, r)
	default:
		return fmt.Errorf("process dump for driver %s not supported", driver)
	}
}

// ProcessInfoSchema invokes process infoschema function from a sql package based on driver selected.
func ProcessInfoSchema(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	case MYSQL:
		return mysql.ProcessInfoSchema(conv, db, os.Getenv("MYSQLDATABASE"))
	case POSTGRES:
		return postgres.ProcessInfoSchema(conv, db)
	default:
		return fmt.Errorf("schema conversion for driver %s not supported", driver)
	}
}

// SetRowStats invokes SetRowStats function from a sql package based on driver selected.
func SetRowStats(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	case MYSQL:
		mysql.SetRowStats(conv, db, os.Getenv("MYSQLDATABASE"))
	case POSTGRES:
		postgres.SetRowStats(conv, db)
	default:
		return fmt.Errorf("Could get rows stats for '%s' driver", driver)
	}
	return nil
}

// ProcessSQLData invokes ProcessSQLData function from a sql package based on driver selected.
func ProcessSQLData(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	case MYSQL:
		mysql.ProcessSQLData(conv, db, os.Getenv("MYSQLDATABASE"))
	case POSTGRES:
		postgres.ProcessSQLData(conv, db)
	default:
		return fmt.Errorf("Data conversion for driver %s is not supported", driver)
	}
	return nil
}
