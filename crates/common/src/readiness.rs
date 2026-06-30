use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum PromotionState {
    ScaffoldOnly,
    Partial,
    HandoffReady,
}

impl fmt::Display for PromotionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ScaffoldOnly => "scaffold-only",
            Self::Partial => "partial",
            Self::HandoffReady => "handoff-ready",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum DriftOverlay {
    NotRun,
    Green,
    Warning,
}

impl fmt::Display for DriftOverlay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::NotRun => "not-run",
            Self::Green => "green",
            Self::Warning => "warning",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum DisplayReadiness {
    ScaffoldOnly,
    Partial,
    HandoffReady,
    DriftWarning,
}

impl fmt::Display for DisplayReadiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ScaffoldOnly => "scaffold-only",
            Self::Partial => "partial",
            Self::HandoffReady => "handoff-ready",
            Self::DriftWarning => "drift-warning",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum PromotionProof {
    Missing,
    Pass,
    Fail,
}

impl PromotionProof {
    fn is_present(self) -> bool {
        self != Self::Missing
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum CoverageProof {
    Missing,
    Complete,
    Gaps,
}

impl CoverageProof {
    fn is_present(self) -> bool {
        self != Self::Missing
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessInput {
    pub rest: PromotionProof,
    pub ws: PromotionProof,
    pub coverage: CoverageProof,
    pub downstream_handoff: PromotionProof,
    pub drift: DriftOverlay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadinessEvaluation {
    pub promotion_state: PromotionState,
    pub drift_overlay: DriftOverlay,
    pub display_readiness: DisplayReadiness,
    pub reason: String,
}

impl Default for ReadinessEvaluation {
    fn default() -> Self {
        evaluate_readiness(ReadinessInput {
            rest: PromotionProof::Missing,
            ws: PromotionProof::Missing,
            coverage: CoverageProof::Missing,
            downstream_handoff: PromotionProof::Missing,
            drift: DriftOverlay::NotRun,
        })
    }
}

pub fn evaluate_readiness(input: ReadinessInput) -> ReadinessEvaluation {
    let promotion_state = if !has_any_promotion_proof(input) {
        PromotionState::ScaffoldOnly
    } else if all_promotion_proofs_pass(input) {
        PromotionState::HandoffReady
    } else {
        PromotionState::Partial
    };

    let display_readiness = match (promotion_state, input.drift) {
        (PromotionState::HandoffReady, DriftOverlay::Warning) => DisplayReadiness::DriftWarning,
        (PromotionState::HandoffReady, _) => DisplayReadiness::HandoffReady,
        (PromotionState::Partial, _) => DisplayReadiness::Partial,
        (PromotionState::ScaffoldOnly, _) => DisplayReadiness::ScaffoldOnly,
    };

    ReadinessEvaluation {
        promotion_state,
        drift_overlay: input.drift,
        display_readiness,
        reason: reason_for(promotion_state, input.drift).to_string(),
    }
}

pub fn evaluate_baseline_readiness(
    promotion_state: PromotionState,
    drift: DriftOverlay,
) -> ReadinessEvaluation {
    let display_readiness = match (promotion_state, drift) {
        (PromotionState::HandoffReady, DriftOverlay::Warning) => DisplayReadiness::DriftWarning,
        (PromotionState::HandoffReady, _) => DisplayReadiness::HandoffReady,
        (PromotionState::Partial, _) => DisplayReadiness::Partial,
        (PromotionState::ScaffoldOnly, _) => DisplayReadiness::ScaffoldOnly,
    };

    ReadinessEvaluation {
        promotion_state,
        drift_overlay: drift,
        display_readiness,
        reason: reason_for(promotion_state, drift).to_string(),
    }
}

fn has_any_promotion_proof(input: ReadinessInput) -> bool {
    input.rest.is_present()
        || input.ws.is_present()
        || input.coverage.is_present()
        || input.downstream_handoff.is_present()
}

fn all_promotion_proofs_pass(input: ReadinessInput) -> bool {
    input.rest == PromotionProof::Pass
        && input.ws == PromotionProof::Pass
        && input.coverage == CoverageProof::Complete
        && input.downstream_handoff == PromotionProof::Pass
}

fn reason_for(promotion_state: PromotionState, drift: DriftOverlay) -> &'static str {
    match (promotion_state, drift) {
        (PromotionState::ScaffoldOnly, DriftOverlay::NotRun) => "no promotion proof exists yet",
        (PromotionState::ScaffoldOnly, _) => {
            "no promotion proof exists yet; drift audit is ignored until the connector is handoff-ready"
        }
        (PromotionState::Partial, _) => {
            "some promotion proof exists, but REST, WS, coverage, and downstream handoff are not all passing"
        }
        (PromotionState::HandoffReady, DriftOverlay::Warning) => {
            "connector is promotion-ready, but live drift audit is warning"
        }
        (PromotionState::HandoffReady, _) => {
            "all promotion proofs pass; drift is not currently warning"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        rest: PromotionProof,
        ws: PromotionProof,
        coverage: CoverageProof,
        downstream_handoff: PromotionProof,
        drift: DriftOverlay,
    ) -> ReadinessInput {
        ReadinessInput {
            rest,
            ws,
            coverage,
            downstream_handoff,
            drift,
        }
    }

    #[test]
    fn no_promotion_proof_is_scaffold_only() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Missing,
            PromotionProof::Missing,
            CoverageProof::Missing,
            PromotionProof::Missing,
            DriftOverlay::NotRun,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::ScaffoldOnly);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::ScaffoldOnly);
    }

    #[test]
    fn partial_when_some_promotion_proof_exists() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Pass,
            PromotionProof::Missing,
            CoverageProof::Missing,
            PromotionProof::Missing,
            DriftOverlay::NotRun,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::Partial);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::Partial);
    }

    #[test]
    fn handoff_ready_when_all_promotion_proofs_pass() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Pass,
            PromotionProof::Pass,
            CoverageProof::Complete,
            PromotionProof::Pass,
            DriftOverlay::Green,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::HandoffReady);
        assert_eq!(evaluation.drift_overlay, DriftOverlay::Green);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::HandoffReady);
    }

    #[test]
    fn drift_warning_only_over_handoff_ready() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Pass,
            PromotionProof::Pass,
            CoverageProof::Complete,
            PromotionProof::Pass,
            DriftOverlay::Warning,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::HandoffReady);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::DriftWarning);
    }

    #[test]
    fn drift_before_promotion_does_not_promote_scaffold() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Missing,
            PromotionProof::Missing,
            CoverageProof::Missing,
            PromotionProof::Missing,
            DriftOverlay::Warning,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::ScaffoldOnly);
        assert_eq!(evaluation.drift_overlay, DriftOverlay::Warning);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::ScaffoldOnly);
    }

    #[test]
    fn degraded_ready_connector_is_partial_even_with_drift_warning() {
        let evaluation = evaluate_readiness(input(
            PromotionProof::Fail,
            PromotionProof::Pass,
            CoverageProof::Complete,
            PromotionProof::Pass,
            DriftOverlay::Warning,
        ));
        assert_eq!(evaluation.promotion_state, PromotionState::Partial);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::Partial);
    }

    #[test]
    fn baseline_drift_warning_preserves_promotion_state() {
        let evaluation =
            evaluate_baseline_readiness(PromotionState::HandoffReady, DriftOverlay::Warning);
        assert_eq!(evaluation.promotion_state, PromotionState::HandoffReady);
        assert_eq!(evaluation.display_readiness, DisplayReadiness::DriftWarning);
    }
}
