# Structural signature fixtures

These goldens lock the `plantree.StructuralSignature` canonical encoding.

- Version prefix: `spannerplan.structural_signature.v1`
- Equality of the full string is the interchange contract for a given version
- The encoding ignores plan-node IDs and execution statistics
- DAG reuse is expanded as ordered visible occurrences (same budgets as `ProcessPlan`)
- Identical operators / shared subtrees can collide; matching layers must expose ambiguity

`dca.signature.txt` is generated from `plantree/reference/testdata/dca.yaml`.
