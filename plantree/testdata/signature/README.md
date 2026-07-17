# Structural signature fixtures

These goldens lock the `plantree.StructuralSignature` canonical encoding.

- Version prefix: `spannerplan.structural_signature.v1alpha1`
- The encoding is byte-length-framed so included values cannot collide through
  delimiters; the golden is a regression check for this alpha revision
- Equality is meaningful only for signatures made by this alpha revision; the
  encoding may change during the alpha and is not an interchange contract
- The encoding ignores plan-node IDs and execution statistics
- DAG reuse is expanded as ordered visible occurrences (same budgets as `ProcessPlan`)
- Identical operators / shared subtrees can collide; matching layers must expose ambiguity

`dca.signature.txt` is generated from `plantree/reference/testdata/dca.yaml`.
