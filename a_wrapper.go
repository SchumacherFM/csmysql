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

package csmysql

import "github.com/corestoreio/errors"

// This wrapper makes the private types public by keeping the original code
// untouched to perform later a quick and easy update from the original github
// repository.

// MysqlConn exported for the sake of accessibility. You cannot create this type
// on your own. Use NewDB.
type MysqlConn struct {
	*mysqlConn
}

func (mc *MysqlConn) SystemVar(name string) (int, error) {
	raw, err := mc.getSystemVar(name)
	return stringToInt(raw), err
}

func newMysqlConn(dsn string) (*MysqlConn, error) {
	dc, err := (new(MySQLDriver)).Open(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "[csmysql] MySQLDriver.Open")
	}
	return &MysqlConn{
		mysqlConn: dc.(*mysqlConn),
	}, nil
}

// DB manages pool of connection
type DB struct {
	dsn   string
	conns chan *MysqlConn // cough cough cough
}

// NewDB initializes pool of connections but doesn't
// establishes connection to DB.
//
// Pool size is fixed and can't be resized later.
// DataSource parameter has the following format:
// [username[:password]@][protocol[(address)]]/dbname
func NewDB(dataSource string, pool int) *DB {
	return &DB{
		dsn:   dataSource,
		conns: make(chan *MysqlConn, pool),
	}
}

// GetConn gets connection from the pool if there is one or
// establishes a new one.This method always returns the connection
// regardless the pool size. When DB is closed, this method
// returns ErrClosedDB error.
func (db *DB) GetConn() (*MysqlConn, error) {
	select {
	case conn, more := <-db.conns:
		if !more {
			return nil, errors.NewAlreadyClosedf("[csmysql] Connection closed")
		}
		return conn, nil
	default:
		return newMysqlConn(db.dsn)
	}
}

// PutConn returns connection to the pool. When pool is reached,
// connection is closed and won't be further reused.
// If connection is already closed, PutConn will discard it
// so it's safe to return closed connection to the pool.
func (db *DB) PutConn(conn *MysqlConn) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = conn.Close()
			return
		}
	}()

	// TODO check broken  or closed connection which shouldn't be in a pool

	select {
	case db.conns <- conn:
	default:
		err = conn.Close()
	}
	return
}

// Close closes all connections in a pool and
// doesn't allow to establish new ones to DB any more.
// Returns slice of errors if any occurred.
func (db *DB) Close() (errs []error) {
	close(db.conns)
	for {
		conn, more := <-db.conns
		if more {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		} else {
			break
		}
	}
	return errs
}
