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

package main

import (
	"net/url"
	"strings"
	"testing"
)

func TestGitHubContentsURLKeepsRefInQuery(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		wantPath string
	}{
		{name: "fragment delimiter", filePath: "dir/go.mod#snapshot", wantPath: "/repos/apstndb/spannerplanviz/contents/dir/go.mod#snapshot"},
		{name: "query delimiter", filePath: "dir/go.mod?snapshot", wantPath: "/repos/apstndb/spannerplanviz/contents/dir/go.mod?snapshot"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := githubContentsURL("apstndb/spannerplanviz", "v0.10.2#fixed", tt.filePath)
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got)
			if err != nil {
				t.Fatal(err)
			}
			if u.Path != tt.wantPath {
				t.Fatalf("Path = %q, want %q (URL %q)", u.Path, tt.wantPath, got)
			}
			if u.Fragment != "" {
				t.Fatalf("Fragment = %q, want empty (URL %q)", u.Fragment, got)
			}
			if gotRef := u.Query().Get("ref"); gotRef != "v0.10.2#fixed" {
				t.Fatalf("ref = %q, want exact ref (URL %q)", gotRef, got)
			}
			if strings.Contains(got, tt.filePath) {
				t.Fatalf("URL contains unescaped path delimiter %q: %q", tt.filePath, got)
			}
		})
	}
}

func TestGitHubContentsURLRejectsMalformedRepo(t *testing.T) {
	for _, repo := range []string{"", "apstndb", "apstndb/repo/extra"} {
		if _, err := githubContentsURL(repo, "main", "go.mod"); err == nil {
			t.Fatalf("githubContentsURL(%q) unexpectedly succeeded", repo)
		}
	}
}
