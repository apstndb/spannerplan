// Copyright 2026 apstndb
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

package ecosystem_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/apstndb/spannerplan/ecosystem"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}

func TestMatrixLoadsAndValidates(t *testing.T) {
	m, err := ecosystem.LoadMatrix(filepath.Join(repoRoot(t), "ecosystem"))
	if err != nil {
		t.Fatalf("LoadMatrix: %v", err)
	}
	if m.AsOf == "" {
		t.Fatal("as_of empty")
	}
	foundViewer := false
	for _, r := range m.Roles {
		if r.ID == "spanner-plan-viewer" {
			foundViewer = true
			if r.Repo != nil {
				t.Fatalf("viewer must remain unpublished (repo=%v)", *r.Repo)
			}
		}
	}
	if !foundViewer {
		t.Fatal("expected spanner-plan-viewer role")
	}
	if len(m.CanaryTargets) == 0 {
		t.Fatal("expected canary targets")
	}
	for _, c := range m.CanaryTargets {
		if c.Repo == "" || c.Ref == "" {
			t.Fatalf("canary target missing repo/ref: %+v", c)
		}
	}
}

func TestECOSYSTEMMarkdownMatchesMatrix(t *testing.T) {
	if err := ecosystem.CheckDocument(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
}

func TestCanaryWorkflowUsesPipefail(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "ecosystem-canary.yml"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "shell: bash\n        run: |\n          set -euo pipefail\n          go run ./ecosystem/cmd/canary -live 2>&1 | tee canary-output.txt"
	if !strings.Contains(string(workflow), want) {
		t.Fatalf("ecosystem canary must use explicit bash with pipefail before tee: %q", want)
	}
}

func TestRenderRoundTrip(t *testing.T) {
	root := t.TempDir()
	ecoDir := filepath.Join(root, "ecosystem")
	if err := os.MkdirAll(ecoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcMatrix, err := os.ReadFile(filepath.Join(repoRoot(t), "ecosystem", "matrix.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ecoDir, "matrix.json"), srcMatrix, 0o644); err != nil {
		t.Fatal(err)
	}

	doc := `# title

## Roles

<!-- ecosystem-roles:begin -->
old roles
<!-- ecosystem-roles:end -->

## Matrix

<!-- ecosystem-matrix:begin -->
old matrix
<!-- ecosystem-matrix:end -->
`
	if err := os.WriteFile(filepath.Join(root, "ECOSYSTEM.md"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ecosystem.WriteDocumentSections(root); err != nil {
		t.Fatalf("WriteDocumentSections: %v", err)
	}
	if err := ecosystem.CheckDocument(root); err != nil {
		t.Fatalf("CheckDocument after write: %v", err)
	}
}
