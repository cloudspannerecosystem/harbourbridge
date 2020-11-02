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

package postgres

import (
	"database/sql"
	"fmt"
	"math/bits"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	_ "github.com/lib/pq" // we will use database/sql package instead of using this package directly

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// TODO: All of the queries to get tables and table data should be in
// a single transaction to ensure we obtain a consistent snapshot of
// schema information and table data (pg_dump does something
// similar).

// ProcessInfoSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessInfoSchema(conv *internal.Conv, db *sql.DB) error {
	tables, err := getTables(db)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := processTable(conv, db, t); err != nil {
			return err
		}
	}
	schemaToDDL(conv)
	conv.AddPrimaryKeys()
	return nil
}

// ProcessSQLData performs data conversion for source database
// 'db'. For each table, we extract data using a "SELECT *" query,
// convert the data to Spanner data (based on the source and Spanner
// schemas), and write it to Spanner.  If we can't get/process data
// for a table, we skip that table and process the remaining tables.
//
// Note that the database/sql library has a somewhat complex model for
// returning data from rows.Scan. Scalar values can be returned using
// the native value used by the underlying driver (by passing
// *interface{} to rows.Scan), or they can be converted to specific go
// types. Array values are always returned as []byte, a string
// encoding of the array values. This string encoding is
// database/driver specific. For example, for PostgreSQL, array values
// are returned in the form "{v1,v2,..,vn}", where each v1,v2,...,vn
// is a PostgreSQL encoding of the respective array value.
//
// We choose to do all type conversions explicitly ourselves so that
// we can generate more targeted error messages: hence we pass
// *interface{} parameters to row.Scan.
func ProcessSQLData(conv *internal.Conv, db *sql.DB) {
	// TODO: refactor to use the set of tables computed by
	// ProcessInfoSchema instead of computing them again.
	tables, err := getTables(db)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		// PostgreSQL schema and name can be arbitrary strings.
		// Ideally we would pass schema/name as a query parameter,
		// but PostgreSQL doesn't support this. So we quote it instead.
		q := fmt.Sprintf(`SELECT * FROM "%s"."%s";`, t.schema, t.name)
		rows, err := db.Query(q)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get data for table: %s", err))
			continue
		}
		defer rows.Close()
		srcTable := buildTableName(t.schema, t.name)
		srcCols, err1 := rows.Columns()
		spTable, err2 := internal.GetSpannerTable(conv, srcTable)
		spCols, err3 := internal.GetSpannerCols(conv, srcTable, srcCols)
		spSchema, ok1 := conv.SpSchema[spTable]
		srcSchema, ok2 := conv.SrcSchema[srcTable]
		if err1 != nil || err2 != nil || err3 != nil || !ok1 || !ok2 {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s: err1=%s, err2=%s, err3=%s, ok1=%t, ok2=%t",
				srcTable, err1, err2, err3, ok1, ok2))
			continue
		}
		v, iv := buildVals(len(srcCols))
		for rows.Next() {
			err := rows.Scan(iv...)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
				// Scan failed, so we don't have any data to add to bad rows.
				conv.StatsAddBadRow(srcTable, conv.DataMode())
				continue
			}
			cvtCols, cvtVals, err := ConvertSQLRow(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, v)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
				conv.StatsAddBadRow(srcTable, conv.DataMode())
				conv.CollectBadRow(srcTable, srcCols, valsToStrings(v))
				continue
			}
			conv.WriteRow(srcTable, spTable, cvtCols, cvtVals)
		}
	}
}

// ConvertSQLRow performs data conversion for a single row of data
// returned from a 'SELECT *' query. ConvertSQLRow assumes that
// srcCols, spCols and srcVals all have the same length. Note that
// ConvertSQLRow returns cols as well as converted values. This is
// because cols can change when we add a column (synthetic primary
// key) or because we drop columns (handling of NULL values).
func ConvertSQLRow(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, srcVals []interface{}) ([]string, []interface{}, error) {
	var vs []interface{}
	var cs []string
	for i := range srcCols {
		srcCd, ok1 := srcSchema.ColDefs[srcCols[i]]
		spCd, ok2 := spSchema.ColDefs[spCols[i]]
		if !ok1 || !ok2 {
			return nil, nil, fmt.Errorf("data conversion: can't find schema for column %s of table %s", srcCols[i], srcTable)
		}
		if srcVals[i] == nil {
			continue // Skip NULL values (nil is used by database/sql to represent NULL values).
		}
		var spVal interface{}
		var err error
		if spCd.T.IsArray {
			spVal, err = cvtSQLArray(conv, srcCd, spCd, srcVals[i])
		} else {
			spVal, err = cvtSQLScalar(conv, srcCd, spCd, srcVals[i])
		}
		if err != nil { // Skip entire row if we hit error.
			return nil, nil, fmt.Errorf("can't convert sql data for column %s of table %s: %w", srcCols[i], srcTable, err)
		}
		vs = append(vs, spVal)
		cs = append(cs, srcCols[i])
	}
	if aux, ok := conv.SyntheticPKeys[spTable]; ok {
		cs = append(cs, aux.Col)
		vs = append(vs, int64(bits.Reverse64(uint64(aux.Sequence))))
		aux.Sequence++
		conv.SyntheticPKeys[spTable] = aux
	}
	return cs, vs, nil
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, db *sql.DB) {
	// TODO: refactor to use the set of tables computed by
	// ProcessInfoSchema instead of computing them again.
	tables, err := getTables(db)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		// PostgreSQL schema and name can be arbitrary strings.
		// Ideally we would pass schema/name as a query parameter,
		// but PostgreSQL doesn't support this. So we quote it instead.
		q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s";`, t.schema, t.name)
		tableName := buildTableName(t.schema, t.name)
		rows, err := db.Query(q)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		defer rows.Close()
		var count int64
		if rows.Next() {
			err := rows.Scan(&count)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Can't get row count: %s", err))
				continue
			}
			conv.Stats.Rows[tableName] += count
		}
	}
}

type schemaAndName struct {
	schema string // PostgreSQL schema (aka namespace for PostgreSQL objects).
	name   string
}

func getTables(db *sql.DB) ([]schemaAndName, error) {
	ignored := make(map[string]bool)
	// Ignore all system tables: we just want to convert user tables.
	for _, s := range []string{"information_schema", "postgres", "pg_catalog", "pg_temp_1", "pg_toast", "pg_toast_temp_1"} {
		ignored[s] = true
	}
	q := "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'"
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableSchema, tableName string
	var tables []schemaAndName
	for rows.Next() {
		rows.Scan(&tableSchema, &tableName)
		if !ignored[tableSchema] {
			tables = append(tables, schemaAndName{schema: tableSchema, name: tableName})
		}
	}
	return tables, nil
}

func processTable(conv *internal.Conv, db *sql.DB, table schemaAndName) error {
	cols, err := getColumns(table, db)
	if err != nil {
		return fmt.Errorf("couldn't get schema for table %s.%s: %s", table.schema, table.name, err)
	}
	defer cols.Close()
	primaryKeys, constraints, err := getConstraints(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.schema, table.name, err)
	}
	foreignKeys, err := getForeignKeys(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.schema, table.name, err)
	}
	colDefs, colNames := processColumns(conv, cols, constraints)
	name := buildTableName(table.schema, table.name)
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
	}
	conv.SrcSchema[name] = schema.Table{
		Name:        name,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
		ForeignKeys: foreignKeys}
	return nil
}

func getColumns(table schemaAndName, db *sql.DB) (*sql.Rows, error) {
	q := `SELECT c.column_name, c.data_type, e.data_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale
              FROM information_schema.COLUMNS c LEFT JOIN information_schema.element_types e
                 ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
                     = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
              where table_schema = $1 and table_name = $2 ORDER BY c.ordinal_position;`
	return db.Query(q, table.schema, table.name)
}

func processColumns(conv *internal.Conv, cols *sql.Rows, constraints map[string][]string) (map[string]schema.Column, []string) {
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType, isNullable string
	var colDefault, elementDataType sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &elementDataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		unique := false
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// c can be UNIQUE, PRIMARY KEY, FOREIGN KEY,
			// or CHECK (based on msql, sql server, postgres docs).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "UNIQUE":
				unique = true
			case "FOREIGN KEY":
				ignored.ForeignKey = true
			case "CHECK":
				ignored.Check = true
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, elementDataType, charMaxLen, numericPrecision, numericScale),
			NotNull: toNotNull(conv, isNullable),
			Unique:  unique,
			Ignored: ignored,
		}
		colDefs[colName] = c
		colNames = append(colNames, colName)
	}
	return colDefs, colNames
}

// getConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func getConstraints(conv *internal.Conv, db *sql.DB, table schemaAndName) ([]string, map[string][]string, error) {
	q := `SELECT k.COLUMN_NAME, t.CONSTRAINT_TYPE
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
                  ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
              WHERE k.TABLE_SCHEMA = $1 AND k.TABLE_NAME = $2 ORDER BY k.ordinal_position;`
	rows, err := db.Query(q, table.schema, table.name)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var primaryKeys []string
	var col, constraint string
	m := make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&col, &constraint)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			conv.Unexpected(fmt.Sprintf("Got empty col or constraint"))
			continue
		}
		switch constraint {
		case "PRIMARY KEY":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// getForeignKeys return list all the foreign keys constraints.
func getForeignKeys(conv *internal.Conv, db *sql.DB, table schemaAndName) (foreignKeys []schema.Fkey, err error) {
	refTables, err := getRefTables(db, table)
	if err != nil {
		return nil, err
	}
	for _, refTable := range refTables {
		fkey, err := getForeignKey(conv, db, table, refTable)
		if err != nil {
			return nil, err
		}
		foreignKeys = append(foreignKeys, fkey)
	}
	return foreignKeys, nil
}

// getRefTables return the list of referenced tables for
// the selected schema and table.
func getRefTables(db *sql.DB, table schemaAndName) ([]schemaAndName, error) {
	q := `SELECT c.TABLE_SCHEMA,c.TABLE_NAME 
		FROM INFORMATION_SCHEMA.constraint_table_usage c
		INNER JOIN INFORMATION_SCHEMA.table_constraints t
			ON c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
		WHERE constraint_type='FOREIGN KEY' 
			AND t.table_schema=$1
			AND t.table_name=$2`
	rows, err := db.Query(q, table.schema, table.name)
	if err != nil {
		return nil, fmt.Errorf("couldn't get reference tables: %w", err)
	}
	defer rows.Close()
	var refTableSchema, refTableName string
	var refTables []schemaAndName
	for rows.Next() {
		rows.Scan(&refTableSchema, &refTableName)
		refTables = append(refTables, schemaAndName{schema: refTableSchema, name: refTableName})
	}
	return refTables, nil
}

// getForeignKey returns the foreign key constraint for
// a particular referenced table in the selected table
// and database.
func getForeignKey(conv *internal.Conv, db *sql.DB, table schemaAndName, refTable schemaAndName) (schema.Fkey, error) {
	q := `SELECT k.column_name,ref.column_name
		FROM INFORMATION_SCHEMA.referential_constraints as r
		INNER JOIN INFORMATION_SCHEMA.key_column_usage as ref
			ON  ref.constraint_catalog = r.unique_constraint_catalog
			AND ref.constraint_schema = r.unique_constraint_schema
			AND ref.constraint_name = r.unique_constraint_name
		INNER JOIN INFORMATION_SCHEMA.key_column_usage as k
			ON  k.constraint_catalog = r.constraint_catalog
			AND k.constraint_schema = r.constraint_schema
			AND k.constraint_name = r.constraint_name
			AND k.position_in_unique_constraint = ref.ordinal_position
		WHERE k.table_schema=$1 
			AND k.table_name=$2
			AND ref.table_schema=$3 
			AND ref.table_name=$4
		ORDER BY ref.ordinal_position`
	rows, err := db.Query(q, table.schema, table.name, refTable.schema, refTable.name)
	if err != nil {
		return schema.Fkey{}, err
	}
	defer rows.Close()
	var col, refCol string
	var cols, refCols []string
	for rows.Next() {
		err := rows.Scan(&col, &refCol)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		cols = append(cols, col)
		refCols = append(refCols, refCol)
	}
	return schema.Fkey{Column: cols,
		ReferTable:  buildTableName(refTable.schema, refTable.name),
		ReferColumn: refCols}, nil
}

func toType(dataType string, elementDataType sql.NullString, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "ARRAY" && elementDataType.Valid:
		return schema.Type{Name: elementDataType.String, ArrayBounds: []int64{-1}}
		// TODO: handle error cases.
		// TODO: handle case of multiple array bounds.
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "numeric" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "numeric" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

func toNotNull(conv *internal.Conv, isNullable string) bool {
	switch isNullable {
	case "YES":
		return false
	case "NO":
		return true
	}
	conv.Unexpected(fmt.Sprintf("isNullable column has unknown value: %s", isNullable))
	return false
}

func cvtSQLArray(conv *internal.Conv, srcCd schema.Column, spCd ddl.ColumnDef, val interface{}) (interface{}, error) {
	a, ok := val.([]byte)
	if !ok {
		return nil, fmt.Errorf("can't convert array values to []byte")
	}
	return convArray(spCd.T, srcCd.Type.Name, conv.Location, string(a))
}

// cvtSQLScalar converts a values returned from a SQL query to a
// Spanner value.  In principle, we could just hand the values we get
// from the driver over to Spanner and have the Spanner client handle
// conversions and errors. However we handle the conversions
// explicitly ourselves so that we can generate more targeted error
// messages. Note that the caller is responsible for handling nil
// values (used to represent NULL). We handle each of the remaining
// cases of values returned by the database/sql library:
//    bool
//    []byte
//    int64
//    float64
//    string
//    time.Time
func cvtSQLScalar(conv *internal.Conv, srcCd schema.Column, spCd ddl.ColumnDef, val interface{}) (interface{}, error) {
	switch spCd.T.Name {
	case ddl.Bool:
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			return convBool(v)
		}
	case ddl.Bytes:
		switch v := val.(type) {
		case []byte:
			return v, nil
		}
	case ddl.Date:
		// The PostgreSQL driver uses time.Time to represent
		// dates.  Note that the database/sql library doesn't
		// document how dates are represented, so maybe this
		// isn't a driver issue, but a generic database/sql
		// issue.  We explicitly convert from time.Time to
		// civil.Date (used by the Spanner client library).
		switch v := val.(type) {
		case string:
			return convDate(v)
		case time.Time:
			return civil.DateOf(v), nil
		}
	case ddl.Int64:
		switch v := val.(type) {
		case []byte: // Parse as int64.
			return convInt64(string(v))
		case int64:
			return v, nil
		case float64: // Truncate.
			return int64(v), nil
		case string: // Parse as int64.
			return convInt64(v)
		}
	case ddl.Float64:
		switch v := val.(type) {
		case []byte: // Note: PostgreSQL uses []byte for numeric.
			return convFloat64(string(v))
		case int64:
			return float64(v), nil
		case float64:
			return v, nil
		case string:
			return convFloat64(v)
		}
	case ddl.String:
		switch v := val.(type) {
		case bool:
			return strconv.FormatBool(v), nil
		case []byte:
			return string(v), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		case string:
			return v, nil
		case time.Time:
			return v.String(), nil
		}
	case ddl.Timestamp:
		switch v := val.(type) {
		case string:
			return convTimestamp(srcCd.Type.Name, conv.Location, v)
		case time.Time:
			return v, nil
		}
	}
	return nil, fmt.Errorf("can't convert value of type %s to Spanner type %s", reflect.TypeOf(val), reflect.TypeOf(spCd.T))
}

// buildVals contructs interface{} value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []interface{}, iv []interface{}) {
	v = make([]interface{}, n)
	for i := range v {
		iv = append(iv, &v[i])
	}
	return v, iv
}

func valsToStrings(vals []interface{}) []string {
	toString := func(val interface{}) string {
		if val == nil {
			return "NULL"
		}
		switch v := val.(type) {
		case *interface{}:
			val = *v
		}
		return fmt.Sprintf("%v", val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}

func buildTableName(schema, name string) string {
	if schema == "public" { // Drop 'public' prefix.
		return name
	}
	return fmt.Sprintf("%s.%s", schema, name)
}
