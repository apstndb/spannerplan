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

// Command canary checks the integrity of public consumer go.mod pins recorded
// in ecosystem/matrix.json.
//
// It is intentionally offline unless -live is set. Live mode fetches only the
// observed rows marked canary=true and never touches local-only viewers. It
// does not resolve current downstream branch or release refs.
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/apstndb/spannerplan/ecosystem"
)

func main() {
	live := flag.Bool("live", false, "fetch pinned public go.mod files and verify recorded requires")
	root := flag.String("root", "", "repository root (default: cwd)")
	flag.Parse()

	repoRoot := *root
	if repoRoot == "" {
		wd, err := os.Getwd()
		if err != nil {
			fail(err)
		}
		repoRoot = wd
	}
	if filepath.Base(repoRoot) == "ecosystem" {
		repoRoot = filepath.Dir(repoRoot)
	}

	if err := ecosystem.CheckDocument(repoRoot); err != nil {
		fail(err)
	}
	fmt.Println("ECOSYSTEM.md matches ecosystem/matrix.json")

	if !*live {
		fmt.Println("skipping live pinned-ref integrity check (pass -live)")
		return
	}

	m, err := ecosystem.LoadMatrix(filepath.Join(repoRoot, "ecosystem"))
	if err != nil {
		fail(err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	var failed int
	for _, t := range m.CanaryTargets() {
		if err := checkTarget(client, t); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s@%s: %v\n", t.Repo, t.Ref, err)
			failed++
			continue
		}
		fmt.Printf("ok %s@%s\n", t.Repo, t.Ref)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func checkTarget(client *http.Client, t ecosystem.CanaryTarget) error {
	raw, err := fetchGitHubFile(client, t.Repo, t.Ref, t.Path)
	if err != nil {
		return err
	}
	requires := parseGoModRequires(string(raw))
	for mod, want := range t.ExpectRequire {
		got, ok := requires[mod]
		if !ok {
			return fmt.Errorf("missing require %s (want %s)", mod, want)
		}
		if got != want {
			return fmt.Errorf("%s: got %s, want %s", mod, got, want)
		}
	}
	return nil
}

func fetchGitHubFile(client *http.Client, repo, ref, path string) ([]byte, error) {
	u := fmt.Sprintf("https://api.github.com/repos/%s/contents/%s?ref=%s",
		repo, path, url.QueryEscape(ref))
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if tok := os.Getenv("GITHUB_TOKEN"); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github API %s: %s", resp.Status, truncate(string(body), 200))
	}
	var payload struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	if payload.Encoding != "base64" {
		return nil, fmt.Errorf("unexpected encoding %q", payload.Encoding)
	}
	return base64.StdEncoding.DecodeString(strings.ReplaceAll(payload.Content, "\n", ""))
}

var requireLine = regexp.MustCompile(`^\s*([^\s]+)\s+(v[^\s]+)`)

func parseGoModRequires(src string) map[string]string {
	out := make(map[string]string)
	inBlock := false
	for _, line := range strings.Split(src, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "require (") {
			inBlock = true
			continue
		}
		if inBlock {
			if trim == ")" {
				inBlock = false
				continue
			}
			if m := requireLine.FindStringSubmatch(trim); m != nil {
				out[m[1]] = m[2]
			}
			continue
		}
		if strings.HasPrefix(trim, "require ") {
			rest := strings.TrimSpace(strings.TrimPrefix(trim, "require "))
			if m := requireLine.FindStringSubmatch(rest); m != nil {
				out[m[1]] = m[2]
			}
		}
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "ecosystem pinned-ref integrity check: %v\n", err)
	os.Exit(1)
}
