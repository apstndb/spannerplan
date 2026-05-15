package main

import (
	"bytes"
	_ "embed"
	"errors"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/google/go-cmp/cmp"
)

//go:embed testdata/postgres18_analyze.json
var postgres18AnalyzeJSON []byte

func TestRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(nil, bytes.NewReader(postgres18AnalyzeJSON), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v\nstderr:\n%s", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	want := heredoc.Doc(`
+----+------------------------------------------+------+-------+--------------+------------+-----------+
| ID | Operator                                 | Rows | Loops | Time         | Cost       | Plan Rows |
+----+------------------------------------------+------+-------+--------------+------------+-----------+
|  0 | Sort (Sort Key: (sum(o.total)) DESC)     |    3 |     1 |  0.15..0.152 | 2.27..2.28 |         3 |
|  1 | +- Aggregate (Hashed, Group Key: u.city) |    3 |     1 |  0.107..0.11 | 2.21..2.24 |         3 |
| *2 |    +- Hash Join (Inner)                  |    4 |     1 | 0.077..0.081 | 1.08..2.18 |         4 |
| *3 |       +- Seq Scan on orders o            |    5 |     1 | 0.016..0.018 |    0..1.07 |         5 |
|  4 |       +- Hash                            |    3 |     1 | 0.017..0.017 | 1.04..1.04 |         3 |
| *5 |          +- Seq Scan on users u          |    3 |     1 |  0.009..0.01 |    0..1.04 |         3 |
+----+------------------------------------------+------+-------+--------------+------------+-----------+
Predicates(identified by ID):
 2: Hash Cond: (o.user_id = u.id)
 3: Filter: (total > '10'::numeric)
 5: Filter: active
`)
	if diff := cmp.Diff(want, stdout.String()); diff != "" {
		t.Fatalf("stdout mismatch (-want +got):\n%s", diff)
	}
}

func TestRun_CompactWrapped(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"-compact", "-wrap-width=36"}, bytes.NewReader(postgres18AnalyzeJSON), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run() error = %v\nstderr:\n%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "+Hash Join (Inner)") {
		t.Fatalf("stdout does not contain compact tree edge:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Predicates(identified by ID):") {
		t.Fatalf("stdout does not contain predicate appendix:\n%s", stdout.String())
	}
}

func TestRun_InvalidJSONShape(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(nil, strings.NewReader(`{"Plan": {}}`), &stdout, &stderr)
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestRun_UnexpectedPositionalArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"extra"}, bytes.NewReader(postgres18AnalyzeJSON), &stdout, &stderr)
	if err == nil {
		t.Fatal("run() error = nil, want non-nil")
	}
	var usageErr *usageError
	if !errors.As(err, &usageErr) {
		t.Fatalf("run() error = %T, want *usageError", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "unexpected positional arguments: extra\n") {
		t.Fatalf("stderr does not contain unexpected argument error:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage of pgexplainjson:") {
		t.Fatalf("stderr does not contain usage:\n%s", stderr.String())
	}
}
