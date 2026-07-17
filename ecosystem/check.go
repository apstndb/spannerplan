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

// Package ecosystem keeps ECOSYSTEM.md aligned with a machine-readable matrix.
package ecosystem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	rolesBegin  = "<!-- ecosystem-roles:begin -->"
	rolesEnd    = "<!-- ecosystem-roles:end -->"
	matrixBegin = "<!-- ecosystem-matrix:begin -->"
	matrixEnd   = "<!-- ecosystem-matrix:end -->"
	matrixJSON  = "matrix.json"
	ecosystemMD = "ECOSYSTEM.md"
)

// Matrix is the machine-readable source of truth for ECOSYSTEM.md tables.
type Matrix struct {
	AsOf                string            `json:"as_of"`
	Disclaimer          string            `json:"disclaimer"`
	Roles               []Role            `json:"roles"`
	SpannerplanVersions map[string]string `json:"spannerplan_versions"`
	Observed            []Observed        `json:"observed"`
	CanaryTargets       []CanaryTarget    `json:"canary_targets"`
}

// Role describes one ecosystem participant.
type Role struct {
	ID       string  `json:"id"`
	Repo     *string `json:"repo"`
	Language string  `json:"language"`
	Role     string  `json:"role"`
}

// Observed records an observed pin without implying compatibility.
type Observed struct {
	Consumer               string          `json:"consumer"`
	ConsumerRef            string          `json:"consumer_ref"`
	Kind                   string          `json:"kind"`
	Canary                 *bool           `json:"canary,omitempty"`
	Requires               []ModuleRequire `json:"requires,omitempty"`
	ParityGoInstall        string          `json:"parity_go_install,omitempty"`
	FixtureSyncRef         string          `json:"fixture_sync_ref,omitempty"`
	LatestPublishedRelease string          `json:"latest_published_release,omitempty"`
	Notes                  string          `json:"notes,omitempty"`
}

// ModuleRequire is a declared Go module require.
type ModuleRequire struct {
	Module  string `json:"module"`
	Version string `json:"version"`
}

// CanaryTarget is a public pinned ref checked by the live canary.
type CanaryTarget struct {
	Repo          string            `json:"repo"`
	Ref           string            `json:"ref"`
	Path          string            `json:"path"`
	ExpectRequire map[string]string `json:"expect_require"`
}

// LoadMatrix reads ecosystem/matrix.json from dir (repository root or ecosystem/).
func LoadMatrix(dir string) (*Matrix, error) {
	path, err := resolveMatrixPath(dir)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Matrix
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func resolveMatrixPath(dir string) (string, error) {
	candidates := []string{
		filepath.Join(dir, matrixJSON),
		filepath.Join(dir, "ecosystem", matrixJSON),
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c, nil
		}
	}
	return "", fmt.Errorf("matrix.json not found under %s", dir)
}

func (m *Matrix) validate() error {
	if m.AsOf == "" {
		return fmt.Errorf("as_of is required")
	}
	if len(m.Roles) == 0 {
		return fmt.Errorf("roles must not be empty")
	}
	if len(m.Observed) == 0 {
		return fmt.Errorf("observed must not be empty")
	}
	if len(m.CanaryTargets) == 0 {
		return fmt.Errorf("canary_targets must not be empty")
	}
	for i, t := range m.CanaryTargets {
		if t.Repo == "" || t.Ref == "" || t.Path == "" {
			return fmt.Errorf("canary_targets[%d]: repo, ref, and path are required", i)
		}
		if len(t.ExpectRequire) == 0 {
			return fmt.Errorf("canary_targets[%d]: expect_require must not be empty", i)
		}
	}
	return nil
}

// RenderRolesTable renders the markdown roles table body (with header).
func (m *Matrix) RenderRolesTable() string {
	var b strings.Builder
	b.WriteString("| Repo | Role |\n")
	b.WriteString("|---|---|\n")
	for _, r := range m.Roles {
		name := r.ID
		left := name
		if r.Repo != nil && *r.Repo != "" {
			left = fmt.Sprintf("[%s](%s)", name, *r.Repo)
		}
		if r.Language != "" {
			left = fmt.Sprintf("%s (%s)", left, r.Language)
		}
		b.WriteString("| ")
		b.WriteString(left)
		b.WriteString(" | ")
		b.WriteString(r.Role)
		b.WriteString(" |\n")
	}
	return b.String()
}

// RenderObservedTable renders the observed-pins markdown table.
func (m *Matrix) RenderObservedTable() string {
	var b strings.Builder
	b.WriteString("As of ")
	b.WriteString(m.AsOf)
	b.WriteString(":\n\n")
	if m.Disclaimer != "" {
		b.WriteString(m.Disclaimer)
		b.WriteString("\n\n")
	}
	if len(m.SpannerplanVersions) > 0 {
		b.WriteString("spannerplan tags observed while writing this matrix: ")
		parts := make([]string, 0, len(m.SpannerplanVersions))
		if v, ok := m.SpannerplanVersions["latest_non_prerelease"]; ok {
			parts = append(parts, "latest non-prerelease "+v)
		}
		if v, ok := m.SpannerplanVersions["latest_prerelease"]; ok {
			parts = append(parts, "prerelease "+v)
		}
		b.WriteString(strings.Join(parts, "; "))
		b.WriteString(".\n\n")
		b.WriteString("These are v0 releases and do not imply a stable compatibility contract.\n\n")
	}
	b.WriteString("| Consumer | Observed ref | Declared / recorded pins |\n")
	b.WriteString("|---|---|---|\n")
	for _, o := range m.Observed {
		pins := formatObservedPins(o)
		b.WriteString("| ")
		b.WriteString(o.Consumer)
		b.WriteString(" | `")
		b.WriteString(o.ConsumerRef)
		b.WriteString("` | ")
		b.WriteString(pins)
		b.WriteString(" |\n")
	}
	return b.String()
}

func formatObservedPins(o Observed) string {
	var parts []string
	for _, req := range o.Requires {
		parts = append(parts, fmt.Sprintf("`%s %s`", req.Module, req.Version))
	}
	if o.ParityGoInstall != "" {
		parts = append(parts, "parity CI `"+o.ParityGoInstall+"`")
	}
	if o.FixtureSyncRef != "" {
		parts = append(parts, "fixtures synced at `"+o.FixtureSyncRef+"`")
	}
	if o.LatestPublishedRelease != "" {
		parts = append(parts, "latest published `"+o.LatestPublishedRelease+"`")
	}
	if len(parts) == 0 {
		return "_(see notes in matrix.json)_"
	}
	return strings.Join(parts, "; ")
}

// CheckDocument verifies ECOSYSTEM.md marked sections match matrix.json.
func CheckDocument(repoRoot string) error {
	m, err := LoadMatrix(filepath.Join(repoRoot, "ecosystem"))
	if err != nil {
		return err
	}
	docPath := filepath.Join(repoRoot, ecosystemMD)
	doc, err := os.ReadFile(docPath)
	if err != nil {
		return err
	}
	if err := checkSection(string(doc), rolesBegin, rolesEnd, m.RenderRolesTable()); err != nil {
		return fmt.Errorf("%s roles section: %w", ecosystemMD, err)
	}
	if err := checkSection(string(doc), matrixBegin, matrixEnd, m.RenderObservedTable()); err != nil {
		return fmt.Errorf("%s matrix section: %w", ecosystemMD, err)
	}
	return nil
}

func checkSection(doc, begin, end, want string) error {
	start := strings.Index(doc, begin)
	if start < 0 {
		return fmt.Errorf("missing begin marker %q", begin)
	}
	rest := doc[start+len(begin):]
	stopRel := strings.Index(rest, end)
	if stopRel < 0 {
		return fmt.Errorf("missing end marker %q", end)
	}
	got := strings.TrimSpace(rest[:stopRel])
	want = strings.TrimSpace(want)
	if got != want {
		return fmt.Errorf("drift detected; regenerate with `go run ./ecosystem/cmd/render`\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	return nil
}

// WriteDocumentSections rewrites the marked sections in ECOSYSTEM.md from matrix.json.
func WriteDocumentSections(repoRoot string) error {
	m, err := LoadMatrix(filepath.Join(repoRoot, "ecosystem"))
	if err != nil {
		return err
	}
	docPath := filepath.Join(repoRoot, ecosystemMD)
	doc, err := os.ReadFile(docPath)
	if err != nil {
		return err
	}
	updated, err := replaceSection(string(doc), rolesBegin, rolesEnd, "\n"+m.RenderRolesTable())
	if err != nil {
		return err
	}
	updated, err = replaceSection(updated, matrixBegin, matrixEnd, "\n"+m.RenderObservedTable())
	if err != nil {
		return err
	}
	return os.WriteFile(docPath, []byte(updated), 0o644)
}

func replaceSection(doc, begin, end, body string) (string, error) {
	start := strings.Index(doc, begin)
	if start < 0 {
		return "", fmt.Errorf("missing begin marker %q", begin)
	}
	afterBegin := start + len(begin)
	stop := strings.Index(doc[afterBegin:], end)
	if stop < 0 {
		return "", fmt.Errorf("missing end marker %q", end)
	}
	stopAbs := afterBegin + stop
	var b strings.Builder
	b.WriteString(doc[:afterBegin])
	if !strings.HasPrefix(body, "\n") {
		b.WriteByte('\n')
	}
	b.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		b.WriteByte('\n')
	}
	b.WriteString(doc[stopAbs:])
	return b.String(), nil
}
