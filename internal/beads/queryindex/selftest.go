package queryindex

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// Conn is a single database connection that both reads and writes. A
// *sql.Conn satisfies it.
type Conn interface {
	Execer
	Querier
}

// The self-test writes to scratch tables in the store's own database. Every
// name starts with selfTestTable, and selfTestIgnorePattern registers that
// prefix in dolt_ignore, so no Dolt commit records the tables: a commit that
// stages every table, as bd's and the compaction flatten's do, skips them.
const (
	selfTestTable         = "__gc_queryindex_selftest"
	selfTestPlainTable    = selfTestTable + "_plain"
	selfTestIgnorePattern = selfTestTable + "*"
)

// SelfTest holds the server-wide user lock selfTestLock while it runs, so
// self-tests from a controller and a doctor run on one server never write
// the same scratch tables at once. A self-test that waits selfTestLockWait
// seconds without the lock does not run.
const (
	selfTestLock     = "gascity.queryindex.selftest"
	selfTestLockWait = 20
)

// SelfTestResult is a Dolt server's answer to whether it can carry metadata
// indexes on beads' tables.
type SelfTestResult struct {
	// OK reports that every check passed.
	OK bool
	// Failure names the first check that failed and what went wrong.
	Failure string
}

// selfTestStep is one statement of the self-test. A step with a query
// compares the rows it returns, each as one string, to want; any other step
// executes stmt. A step with no check prepares the table, and its failure is
// not a verdict.
type selfTestStep struct {
	check string
	stmt  string
	query string
	want  []string
}

// selfTestRows reads every row of the main scratch table as
// id|body|flag|metadata.j.
const selfTestRows = `SELECT CONCAT_WS('|', id, body, flag, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."j"'))) FROM ` + selfTestTable + ` ORDER BY id`

// selfTestSteps write to a scratch table that carries what the known failures
// depend on: an ON UPDATE CURRENT_TIMESTAMP column, as beads' updated_at is,
// and two metadata indexes, as a store with two indexed keys has. On the Dolt
// releases this was measured against, each of these breaks:
//   - before 2.4.0, adding the index fails;
//   - on 2.4.0 through at least 2.4.2, an upsert that updates an existing row
//     writes values into the wrong columns or drops the update, and reports
//     success; beads creates issues with such an upsert;
//   - on 2.4.1 and 2.4.2, after MODIFY COLUMN changes a column's type, as
//     beads schema migrations do, every aliased UPDATE of the table fails
//     with "table not found", and bd issues one on every create.
var selfTestSteps = []selfTestStep{
	{stmt: "DROP TABLE IF EXISTS " + selfTestTable},
	{stmt: "CREATE TABLE " + selfTestTable + " (id varchar(16) PRIMARY KEY, flag tinyint NOT NULL DEFAULT 0, body longtext, metadata json, updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"},
	{check: "adding a metadata index to a table with an ON UPDATE CURRENT_TIMESTAMP column", stmt: "CREATE INDEX selftest_k ON " + selfTestTable + " ((JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.\"k\"'))))"},
	{check: "adding a second metadata index", stmt: "CREATE INDEX selftest_j ON " + selfTestTable + " ((JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.\"j\"'))))"},
	{check: "inserting a row", stmt: `INSERT INTO ` + selfTestTable + ` (id, body, metadata) VALUES ('a', 'a1', '{"k": "5", "j": "a1"}')`},
	{check: "an upsert that inserts a row", stmt: `INSERT INTO ` + selfTestTable + ` (id, body, metadata) VALUES ('b', 'b1', '{"k": "6", "j": "b1"}') ON DUPLICATE KEY UPDATE body = VALUES(body), metadata = VALUES(metadata)`},
	{check: "an upsert that updates a row", stmt: `INSERT INTO ` + selfTestTable + ` (id, body, metadata) VALUES ('a', 'a2', '{"k": "5", "j": "a2"}') ON DUPLICATE KEY UPDATE body = VALUES(body), metadata = VALUES(metadata)`},
	{check: "INSERT IGNORE of an existing row", stmt: "INSERT IGNORE INTO " + selfTestTable + " (id) VALUES ('b')"},
	{check: "an aliased UPDATE", stmt: "UPDATE " + selfTestTable + " p SET p.flag = 1 WHERE p.id = 'b'"},
	{check: "the rows those writes left", query: selfTestRows, want: []string{"a|a2|0|a2", "b|b1|1|b1"}},
	// Which columns a broken upsert shifts depends on the table's layout, so
	// a second table repeats it with one index and no ON UPDATE column.
	{stmt: "DROP TABLE IF EXISTS " + selfTestPlainTable},
	{stmt: "CREATE TABLE " + selfTestPlainTable + " (id varchar(16) PRIMARY KEY, body longtext, metadata json)"},
	{check: "adding a metadata index to a plain table", stmt: "CREATE INDEX selftest_plain_k ON " + selfTestPlainTable + " ((JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.\"k\"'))))"},
	{check: "inserting into the plain table", stmt: `INSERT INTO ` + selfTestPlainTable + ` (id, body, metadata) VALUES ('a', 'a1', '{"k": "5", "j": "a1"}')`},
	{check: "an upsert that updates a row of the plain table", stmt: `INSERT INTO ` + selfTestPlainTable + ` (id, body, metadata) VALUES ('a', 'a2', '{"k": "5", "j": "a2"}') ON DUPLICATE KEY UPDATE body = VALUES(body), metadata = VALUES(metadata)`},
	{check: "the plain table's row after the upsert", query: `SELECT CONCAT_WS('|', id, body, JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."j"'))) FROM ` + selfTestPlainTable, want: []string{"a|a2|a2"}},
	{check: "narrowing a column with MODIFY COLUMN", stmt: "ALTER TABLE " + selfTestTable + " MODIFY COLUMN body TEXT"},
	{check: "an aliased UPDATE after narrowing a column", stmt: "UPDATE " + selfTestTable + " p SET p.flag = 2 WHERE p.id = 'b'"},
	{check: "widening a column with a default, as beads migration 0049 does", stmt: "ALTER TABLE " + selfTestTable + " MODIFY COLUMN body LONGTEXT DEFAULT ''"},
	{check: "an aliased UPDATE after widening a column", stmt: "UPDATE " + selfTestTable + " p SET p.flag = 3 WHERE p.id = 'b'"},
	{check: "changing an integer column's type", stmt: "ALTER TABLE " + selfTestTable + " MODIFY COLUMN flag INT NOT NULL DEFAULT 0"},
	{check: "an aliased UPDATE after changing an integer column's type", stmt: "UPDATE " + selfTestTable + " p SET p.flag = 4 WHERE p.id = 'b'"},
	{check: "an upsert that updates a row after the column changes", stmt: `INSERT INTO ` + selfTestTable + ` (id, body, metadata) VALUES ('b', 'b2', '{"k": "6", "j": "b2"}') ON DUPLICATE KEY UPDATE body = VALUES(body), metadata = VALUES(metadata)`},
	{check: "inserting a row after the column changes", stmt: `INSERT INTO ` + selfTestTable + ` (id, body, metadata) VALUES ('c', 'c1', '{"k": "7", "j": "c1"}')`},
	{check: "the rows after the column changes", query: selfTestRows, want: []string{"a|a2|0|a2", "b|b2|4|b2", "c|c1|0|c1"}},
	{check: "a lookup through the first metadata index", query: `SELECT id FROM ` + selfTestTable + ` WHERE JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."k"')) = '5'`, want: []string{"a"}},
	{check: "a lookup through the second metadata index", query: `SELECT id FROM ` + selfTestTable + ` WHERE JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."j"')) = 'b2'`, want: []string{"b"}},
}

// SelfTest runs selfTestSteps on scratch tables in conn's current database
// and reports the first check that fails. It returns an error, and no
// verdict, when it cannot run the test at all. It runs every statement on
// conn, under selfTestLock. Before it creates a table it registers the
// tables' dolt_ignore pattern and confirms the pattern is in force, and it
// drops the tables before it returns; the pattern stays.
func SelfTest(ctx context.Context, conn Conn) (result SelfTestResult, err error) {
	locked, err := queryStrings(ctx, conn, fmt.Sprintf("SELECT GET_LOCK(%s, %d)", sqlString(selfTestLock), selfTestLockWait))
	if err != nil {
		return SelfTestResult{}, fmt.Errorf("taking the self-test lock: %w", err)
	}
	if len(locked) != 1 || locked[0] != "1" {
		return SelfTestResult{}, fmt.Errorf("another index self-test held the server's self-test lock for %ds", selfTestLockWait)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if _, dropErr := conn.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+selfTestTable+", "+selfTestPlainTable); dropErr != nil && err == nil {
			result, err = SelfTestResult{}, fmt.Errorf("dropping the self-test tables: %w", dropErr)
		}
		released, unlockErr := queryStrings(cleanupCtx, conn, "SELECT RELEASE_LOCK("+sqlString(selfTestLock)+")")
		if unlockErr == nil && (len(released) != 1 || released[0] != "1") {
			unlockErr = fmt.Errorf("RELEASE_LOCK returned %q", released)
		}
		if unlockErr != nil && err == nil {
			result, err = SelfTestResult{}, fmt.Errorf("releasing the self-test lock: %w", unlockErr)
		}
	}()
	if err := ignoreSelfTestTables(ctx, conn); err != nil {
		return SelfTestResult{}, err
	}
	for _, step := range selfTestSteps {
		var got []string
		var stepErr error
		if step.query != "" {
			got, stepErr = queryStrings(ctx, conn, step.query)
		} else {
			_, stepErr = conn.ExecContext(ctx, step.stmt)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return SelfTestResult{}, ctxErr
		}
		switch {
		case stepErr != nil && step.check == "":
			return SelfTestResult{}, fmt.Errorf("preparing the self-test table: %w", stepErr)
		case stepErr != nil:
			return SelfTestResult{Failure: step.check + " failed: " + stepErr.Error()}, nil
		case step.query != "" && !slices.Equal(got, step.want):
			return SelfTestResult{Failure: fmt.Sprintf("%s: got %q, want %q", step.check, got, step.want)}, nil
		}
	}
	return SelfTestResult{OK: true}, nil
}

// ignoreSelfTestTables registers selfTestIgnorePattern in the database's
// dolt_ignore and confirms Dolt ignores the tables it covers. INSERT IGNORE
// keeps a row an operator already wrote for the pattern, so a row that does
// not ignore the tables stops the self-test before it creates one.
func ignoreSelfTestTables(ctx context.Context, conn Conn) error {
	if _, err := conn.ExecContext(ctx, "INSERT IGNORE INTO dolt_ignore (pattern, ignored) VALUES ("+sqlString(selfTestIgnorePattern)+", 1)"); err != nil {
		return fmt.Errorf("registering the self-test tables in dolt_ignore: %w", err)
	}
	ignored, err := queryStrings(ctx, conn, "SELECT ignored FROM dolt_ignore WHERE pattern = "+sqlString(selfTestIgnorePattern))
	if err != nil {
		return fmt.Errorf("reading the self-test tables' dolt_ignore row: %w", err)
	}
	if len(ignored) != 1 || (ignored[0] != "1" && ignored[0] != "true") {
		return fmt.Errorf("dolt_ignore does not ignore %s (ignored = %q), so a Dolt commit could record the self-test tables", selfTestIgnorePattern, ignored)
	}
	return nil
}

// DoltVersion returns the version of the Dolt server db is connected to.
func DoltVersion(ctx context.Context, db Querier) (string, error) {
	versions, err := queryStrings(ctx, db, "SELECT DOLT_VERSION()")
	if err != nil {
		return "", fmt.Errorf("reading the Dolt version: %w", err)
	}
	if len(versions) != 1 {
		return "", fmt.Errorf("reading the Dolt version: got %d rows, want 1", len(versions))
	}
	return versions[0], nil
}
