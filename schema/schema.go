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

// Package schema provides a generic representation of database
// schemas. Note that our goal is not to faithfully represent all
// aspects of the schema, but just the relevant components for
// conversion to Spanner and reporting on the quality of the
// conversion (this motivates us to keep partial information about
// some features we will report on but not use in the conversion
// e.g. default values, check constraints).
//
// The current version supports PostgreSQL. Expect it to grow as we
// support other databases. We might eventually support the Spanner
// schema, and potentially get rid of the ddl package.
package schema

import (
	"fmt"
	"strconv"
	"strings"
)

// Table represents a database table.
type Table struct {
	Name        string
	ColNames    []string          // List of column names (for predictable iteration order e.g. printing).
	ColDefs     map[string]Column // Details of columns.
	PrimaryKeys []Key
	ForeignKeys []ForeignKey
	Indexes     []Index
}

// Column represents a database column.
// TODO: add support for foreign keys.
type Column struct {
	Name    string
	Type    Type
	NotNull bool
	Ignored Ignored
}

// ForeignKey represents a foreign key.
// Note that the fields onDelete and onUpdate describe actions
// for when keys are deleted or updated. Different source databases
// support different actions. For example, mysql supports RESTRICT,
// CASCADE, SET NULL, NO ACTION, and SET DEFAULT
// (see https://dev.mysql.com/doc/refman/5.6/en/create-table-foreign-keys.html).
type ForeignKey struct {
	Name         string
	Columns      []string
	ReferTable   string
	ReferColumns []string // len(ReferColumns) must be same as len(Columns)
	OnDelete     string
	OnUpdate     string
}

// Key respresents a primary key or index key.
type Key struct {
	Column string
	Desc   bool // By default, order is ASC. Set to true to specifiy DESC.
}

// Index represents a database index.
type Index struct {
	Name   string
	Unique bool
	Keys   []Key
}

// Type represents the type of a column.
type Type struct {
	Name        string
	Mods        []int64 // List of modifiers (aka type parameters e.g. varchar(8) or numeric(6, 4).
	ArrayBounds []int64 // Empty for scalar types.
}

// Ignored represents column properties/constraints that are not
// represented. We drop the details, but retain presence/absence for
// reporting purposes.
type Ignored struct {
	Check         bool
	Identity      bool
	Default       bool
	Exclusion     bool
	ForeignKey    bool
	AutoIncrement bool
}

// Print converts ty to a string suitable for printing.
func (ty Type) Print() string {
	s := ty.Name
	if len(ty.Mods) > 0 {
		var l []string
		for _, x := range ty.Mods {
			l = append(l, strconv.FormatInt(x, 10))
		}
		s = fmt.Sprintf("%s(%s)", s, strings.Join(l, ","))
	}
	if len(ty.ArrayBounds) > 0 {
		l := []string{s}
		for _, x := range ty.ArrayBounds {
			if x == -1 {
				l = append(l, "[]")
			} else {
				l = append(l, fmt.Sprintf("[%d]", x))
			}
		}
		s = strings.Join(l, "")
	}
	return s
}
