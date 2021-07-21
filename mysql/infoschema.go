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

package mysql

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	_ "github.com/go-sql-driver/mysql" // The driver should be used via the database/sql package.
	_ "github.com/lib/pq"
)

// ProcessInfoSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessInfoSchema(conv *internal.Conv, db *sql.DB, dbName string) error {
	tables, err := getTables(db, dbName)
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
// 'db'. For each table, we extract data using a "SELECT (colNamesList)" query,
// convert the data to Spanner data (based on the source and Spanner
// schemas), and write it to Spanner.  If we can't get/process data
// for a table, we skip that table and process the remaining tables.
//
// Using database/sql library we pass *sql.RawBytes to rows.scan.
// RawBytes is a byte slice and values can be easily converted to string.
func ProcessSQLData(conv *internal.Conv, db *sql.DB, dbName string) {
	// TODO: refactor to use the set of tables computed by
	// ProcessInfoSchema instead of computing them again.
	tables, err := getTables(db, dbName)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		srcTable := t.name
		srcSchema, ok := conv.SrcSchema[srcTable]
		if !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		srcCols := srcSchema.ColNames
		if len(srcCols) == 0 {
			conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", t.name))
			continue
		}
		colNameList := buildColNameList(srcSchema, srcCols)
		// MySQL schema and name can be arbitrary strings.
		// Ideally we would pass schema/name as a query parameter,
		// but MySQL doesn't support this. So we quote it instead.
		q := fmt.Sprintf("SELECT %s FROM `%s`.`%s`;", colNameList, t.schema, t.name)
		rows, err := db.Query(q)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", t.name, err))
			continue
		}
		defer rows.Close()
		srcCols, _ = rows.Columns()
		spTable, err := internal.GetSpannerTable(conv, srcTable)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner table : %s", err))
			continue
		}
		spCols, err := internal.GetSpannerCols(conv, srcTable, srcCols)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner columns for table %s : err = %s", t.name, err))
			continue
		}
		spSchema, ok := conv.SpSchema[spTable]
		if !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		v, scanArgs := buildVals(len(srcCols))
		for rows.Next() {
			// get RawBytes from data.
			err = rows.Scan(scanArgs...)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
				// Scan failed, so we don't have any data to add to bad rows.
				conv.StatsAddBadRow(srcTable, conv.DataMode())
				continue
			}
			values := valsToStrings(v)
			ProcessDataRow(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, values)
		}
	}
}

// Building list of column names to support mysql spatial datatypes instead of
// using 'SELECT *' because spatial columns will be fetched using ST_AsText(colName).
func buildColNameList(srcSchema schema.Table, srcColName []string) string {
	var srcColTypes []string
	var colList, colTmpName string
	for _, colName := range srcColName {
		// To handle cases where column name is reserved keyword or having space between words.
		colTmpName = "`" + colName + "`"
		srcColTypes = append(srcColTypes, srcSchema.ColDefs[colName].Type.Name)
		for _, spatial := range MysqlSpatialDataTypes {
			if strings.Contains(strings.ToLower(srcSchema.ColDefs[colName].Type.Name), spatial) {
				colTmpName = "ST_AsText" + "(" + colTmpName + ")" + colTmpName
				break
			}
		}
		colList = colList + colTmpName + ","
	}
	return colList[:len(colList)-1]
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, db *sql.DB, dbName string) {
	tables, err := getTables(db, dbName)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		// MySQL schema and name can be arbitrary strings.
		// Ideally we would pass schema/name as a query parameter,
		// but MySQL doesn't support this. So we quote it instead.
		q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`;", t.schema, t.name)
		tableName := t.name
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
	schema string
	name   string
}

// getTables return list of tables in the selected database.
// Note that sql.DB already effectively has the dbName
// embedded within it (dbName is part of the DSN passed to sql.Open),
// but unfortunately there is no way to extract it from sql.DB.
func getTables(db *sql.DB, dbName string) ([]schemaAndName, error) {
	// In MySQL, schema is the same as database name.
	q := "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and table_schema=?"
	rows, err := db.Query(q, dbName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableName string
	var tables []schemaAndName
	for rows.Next() {
		rows.Scan(&tableName)
		tables = append(tables, schemaAndName{schema: dbName, name: tableName})
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
	indexes, err := getIndexes(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.schema, table.name, err)
	}
	colDefs, colNames := processColumns(conv, cols, constraints)
	name := table.name
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
	}
	conv.SrcSchema[name] = schema.Table{
		Name:        name,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
		Indexes:     indexes,
		ForeignKeys: foreignKeys}
	return nil
}

func getColumns(table schemaAndName, db *sql.DB) (*sql.Rows, error) {
	q := `SELECT c.column_name, c.data_type, c.column_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.extra
              FROM information_schema.COLUMNS c
              where table_schema = ? and table_name = ? ORDER BY c.ordinal_position;`
	return db.Query(q, table.schema, table.name)
}

func processColumns(conv *internal.Conv, cols *sql.Rows, constraints map[string][]string) (map[string]schema.Column, []string) {
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType, isNullable, columnType string
	var colDefault, colExtra sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &columnType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale, &colExtra)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// c can be UNIQUE, PRIMARY KEY, FOREIGN KEY or CHECK
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "CHECK":
				ignored.Check = true
			case "FOREIGN KEY", "PRIMARY KEY", "UNIQUE":
				// Nothing to do here -- these are all handled elsewhere.
			}
		}
		ignored.Default = colDefault.Valid
		if colExtra.String == "auto_increment" {
			ignored.AutoIncrement = true
		}
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, columnType, charMaxLen, numericPrecision, numericScale),
			NotNull: toNotNull(conv, isNullable),
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
                  ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND t.TABLE_NAME=k.TABLE_NAME
              WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? ORDER BY k.ordinal_position;`
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

type fkConstraint struct {
	name    string
	table   string
	refcols []string
	cols    []string
}

// getForeignKeys return list all the foreign keys constraints.
// MySQL supports cross-database foreign key constraints. We ignore
// them because HarbourBridge works database at a time (a specific run
// of HarbourBridge focuses on a specific database) and so we can't handle
// them effectively.
func getForeignKeys(conv *internal.Conv, db *sql.DB, table schemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := `SELECT k.REFERENCED_TABLE_NAME,k.COLUMN_NAME,k.REFERENCED_COLUMN_NAME,k.CONSTRAINT_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t 
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
			ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
			AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
			AND t.TABLE_NAME = k.TABLE_NAME 
			AND k.REFERENCED_TABLE_SCHEMA = k.TABLE_SCHEMA
		WHERE k.TABLE_SCHEMA = ? 
			AND k.TABLE_NAME = ? 
			AND t.CONSTRAINT_TYPE = "FOREIGN KEY" 
		ORDER BY
			k.REFERENCED_TABLE_NAME,
			k.COLUMN_NAME,
			k.ORDINAL_POSITION;`
	rows, err := db.Query(q, table.schema, table.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refCol, refTable, fKeyName string
	fKeys := make(map[string]fkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refTable, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.cols = append(fk.cols, col)
			fk.refcols = append(fk.refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = fkConstraint{name: fKeyName, table: refTable, refcols: []string{refCol}, cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}
	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Name:         fKeys[k].name,
				Columns:      fKeys[k].cols,
				ReferTable:   fKeys[k].table,
				ReferColumns: fKeys[k].refcols})
	}
	return foreignKeys, nil
}

// getIndexes return a list of all indexes for the specified table.
func getIndexes(conv *internal.Conv, db *sql.DB, table schemaAndName) ([]schema.Index, error) {
	q := `SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE
		FROM INFORMATION_SCHEMA.STATISTICS 
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
			AND INDEX_NAME != 'PRIMARY' 
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;`
	rows, err := db.Query(q, table.schema, table.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, nonUnique string
	var collation sql.NullString
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &nonUnique); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (nonUnique == "0")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation.Valid && collation.String == "D")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}
func toType(dataType string, columnType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "set":
		return schema.Type{Name: dataType, ArrayBounds: []int64{-1}}
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "decimal" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "decimal" && numericPrecision.Valid:
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

// buildVals constructs []sql.RawBytes value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []sql.RawBytes, iv []interface{}) {
	v = make([]sql.RawBytes, n)
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice.
	iv = make([]interface{}, len(v))
	for i := range v {
		iv[i] = &v[i]
	}
	return v, iv
}

func valsToStrings(vals []sql.RawBytes) []string {
	toString := func(val sql.RawBytes) string {
		if val == nil {
			return "NULL"
		}
		return string(val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}
