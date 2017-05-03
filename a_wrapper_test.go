// Copyright 2015-2017, Cyrill @ Schumacher.fm and the CoreStore contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package csmysql_test

import (
	"database/sql/driver"
	"io"
	"os"
	"testing"

	"github.com/corestoreio/csfw/storage/csmysql"
	"github.com/corestoreio/csfw/storage/dbr"
)

func TestMySQLDriver_OpenConn(t *testing.T) {
	dbPool := csmysql.NewDB(os.Getenv("CS_DSN"), 5)
	defer func() {
		if errs := dbPool.Close(); errs != nil {
			t.Fatalf("%+v", errs)
		}
	}()

	conn, err := dbPool.GetConn()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer func() {
		if err := dbPool.PutConn(conn); err != nil {
			t.Fatalf("%+v", err)
		}
	}()

	sqlStr, args, err := dbr.NewSelect("*").From("core_config_data").
		Where(dbr.Condition("path LIKE 'carriers%'")).ToSQL()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	rows, err := conn.Query(sqlStr, args.DriverValues())
	if err != nil {
		t.Fatalf("%+v\n%q", err, sqlStr)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("%+v", err)
		}
	}()

	cols := rows.Columns()
	t.Logf("%#v", cols)

	vals := make([]driver.Value, len(cols))
	err = rows.Next(vals)
	for err == nil {
		for i, val := range vals {
			t.Logf("%q -> %s", cols[i], val)
		}
		err = rows.Next(vals)
	}
	if err != nil && err != io.EOF {
		t.Fatalf("%+v", err)
	}
}
