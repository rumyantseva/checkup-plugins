package pq

import (
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // import postgresql driver
	"github.com/sourcegraph/checkup"
)

// schema is the table schema expected by the sqlite3 checkup storage.
const schema = `
CREATE TABLE checks (
    name TEXT NOT NULL PRIMARY KEY,
    timestamp INT8 NOT NULL,
    results TEXT
);
CREATE UNIQUE INDEX idx_checks_timestamp ON checks(timestamp);
`

// Storage is a way to store checkup results in a Storage database.
type Storage struct {
	// DB contains the DB connection
	DB *sqlx.DB

	// Check files older than CheckExpiry will be
	// deleted on calls to Maintain(). If this is
	// the zero value, no old check files will be
	// deleted.
	CheckExpiry time.Duration `json:"check_expiry,omitempty"`
}

// GetIndex returns the list of check results for the database.
func (sql Storage) GetIndex() (map[string]int64, error) {
	idx := make(map[string]int64)
	var check struct {
		Name      string `db:"name"`
		Timestamp int64  `db:"timestamp"`
	}

	rows, err := sql.DB.Queryx(`SELECT name,timestamp FROM "checks"`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.StructScan(&check)
		if err != nil {
			rows.Close()
			return nil, err
		}
		idx[check.Name] = check.Timestamp
	}

	return idx, nil
}

// Fetch fetches results of the check with given name.
func (sql Storage) Fetch(name string) ([]checkup.Result, error) {
	var checkResult []byte
	var results []checkup.Result

	err := sql.DB.Get(&checkResult, `SELECT results FROM "checks" WHERE name=$1 LIMIT 1`, name)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(checkResult, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// Store stores results in the database.
func (sql Storage) Store(results []checkup.Result) error {
	name := *checkup.GenerateFilename()
	contents, err := json.Marshal(results)
	if err != nil {
		return err
	}

	// Insert data
	const insertResults = `INSERT INTO "checks" (name, timestamp, results) VALUES (?, ?, ?)`
	_, err = sql.DB.Exec(insertResults, name, time.Now().UnixNano(), contents)
	return err
}

// Maintain deletes check files that are older than sql.CheckExpiry.
func (sql Storage) Maintain() error {
	if sql.CheckExpiry == 0 {
		return nil
	}

	const st = `DELETE FROM "checks" WHERE timestamp < ?`
	ts := time.Now().Add(-1 * sql.CheckExpiry).UnixNano()
	_, err := sql.DB.Exec(st, ts)
	return err
}

// initialize creates the "checks" table in the database.
func (sql Storage) initialize() error {
	_, err := sql.DB.Exec(schema)
	return err
}
