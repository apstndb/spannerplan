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

// Command render rewrites ECOSYSTEM.md marked sections from ecosystem/matrix.json.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apstndb/spannerplan/ecosystem"
)

func main() {
	root, err := os.Getwd()
	if err != nil {
		fail(err)
	}
	// Allow running from repo root or ecosystem/.
	if filepath.Base(root) == "ecosystem" {
		root = filepath.Dir(root)
	}
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if err := ecosystem.WriteDocumentSections(root); err != nil {
		fail(err)
	}
	fmt.Println("updated ECOSYSTEM.md from ecosystem/matrix.json")
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "ecosystem render: %v\n", err)
	os.Exit(1)
}
