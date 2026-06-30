# Readiness State Model

The readiness prototype answered whether connector lifecycle should be modeled as
`scaffold-only -> partial -> handoff-ready -> drift-warning`.

Verdict:

- `scaffold-only`, `partial`, and `handoff-ready` are promotion states.
- REST proof, WS proof, coverage proof, and downstream handoff proof are
  promotion proofs.
- Drift audit is not promotion proof. It is monitoring over a connector after a
  readiness baseline exists.
- A drift result without any promotion proof must not move a connector from
  `scaffold-only` to `partial`.
- `drift-warning` is display readiness: a warning overlay over a connector whose
  promotion state remains `handoff-ready`.

The throwaway prototype that produced this verdict was deleted after the rules
were lifted into the production readiness evaluation module.
