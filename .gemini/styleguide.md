# spannerplan review guide

Prioritize correctness, API behavior, parsing robustness, output stability, and user-facing CLI behavior over micro-optimizations.

For performance feedback:

- Do not suggest allocation micro-optimizations unless the code is on a demonstrated hot path, inside a performance-critical loop, or the change materially improves asymptotic behavior.
- Avoid comments that only recommend preallocating small slices, hoisting immutable helper values to package scope, or reducing one-time allocations in CLI setup, parsing, and tests unless there is clear evidence that the cost matters.
- Prefer one consolidated performance comment over multiple near-duplicate comments on the same theme.

For this repository:

- Focus on compatibility of rendered output, public API clarity, and robustness of plan parsing / rendering.
- Treat command setup and one-shot CLI parsing paths as low priority for allocation tuning.
