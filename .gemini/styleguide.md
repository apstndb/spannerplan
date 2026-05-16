# spannerplan review guide

Prioritize correctness, API behavior, parsing robustness, output stability, and user-facing CLI behavior over micro-optimizations.

For performance feedback:

- Do not suggest allocation micro-optimizations unless the code is on a demonstrated hot path, inside a performance-critical loop, or the change materially improves asymptotic behavior.
- Avoid comments that only recommend preallocating small slices, hoisting immutable helper values to package scope, or reducing one-time allocations in CLI setup, parsing, and tests unless there is clear evidence that the cost matters.
- Do not suggest replacing expressive standard-library helpers such as `strings.TrimPrefix` with manual slicing solely for performance. Prefer readability unless profiling or a hot path justifies the change.
- Prefer one consolidated performance comment over multiple near-duplicate comments on the same theme.

For this repository:

- Focus on compatibility of rendered output, public API clarity, and robustness of plan parsing / rendering.
- Treat command setup and one-shot CLI parsing paths as low priority for allocation tuning.
- For JSON tags, treat `omitempty` as an API design choice. Check whether nil, empty, and omitted values intentionally have distinct semantics before recommending a tag change.
