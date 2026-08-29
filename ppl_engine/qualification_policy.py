"""V3.1 declaration-driven Qualification compatibility layer.

C2 deliberately keeps the proven V3 PPL classifier as the authoritative
classification composer while extracting its current business rules into a
stable, read-only rule evaluator.  This lets Continuous runs attribute and
inspect Platform Hard Rules, Local Qualification Rules, Local Strategy Rules
and Diagnostic Warnings without changing Simulation execution semantics.

The evaluator owns no database connection, HTTP session, state transition or
remote side effect.  Rule thresholds are resolved from the active
``ppl_classification`` policy; the declaration file therefore does not duplicate
business values that could drift from the current classifier.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

import yaml

from .strategy_contracts import MissingFactAction, QualificationResult, RuleRole

QUALIFICATION_EVALUATOR_VERSION = "V31_QUAL_DECL_001"


class RuleEvaluationStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNRESOLVED = "UNRESOLVED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class QualificationRuleDeclaration:
    rule_id: str
    role: RuleRole
    source: str
    fact: str = ""
    operator: str = ""
    threshold_from: str = ""
    names_from: str = ""
    name_from: str = ""
    aliases: Tuple[str, ...] = ()
    pass_outcomes: Tuple[str, ...] = ("PASS", "NOT_APPLICABLE")
    on_missing: MissingFactAction = MissingFactAction.UNRESOLVED
    failure_code: str = ""
    repairable: bool = False
    effect: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QualificationRuleEvaluation:
    rule_id: str
    role: RuleRole
    status: RuleEvaluationStatus
    actual: Any = None
    expected: Any = None
    failure_code: str = ""
    repairable: bool = False
    effect: str = ""
    source: str = ""
    fact: str = ""
    platform_outcome: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QualificationEvaluationBundle:
    result: QualificationResult
    rules: Tuple[QualificationRuleEvaluation, ...]
    evaluator_version: str
    policy_hash: str

@dataclass(frozen=True)
class QualificationPolicySnapshot:
    classification_policy: Mapping[str, Any]
    integration: Mapping[str, Any]
    policy_hash: str
    source_path: str


_RUNTIME_POLICY_SNAPSHOTS: Dict[str, QualificationPolicySnapshot] = {}


def _key(value: Any) -> str:
    import re
    return re.sub(r"[^A-Z0-9]+", "_", str(value or "").strip().upper()).strip("_")


def _float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return float(parsed) if isinstance(parsed, (int, float)) else None


def _raw_value(row: Mapping[str, Any]) -> Any:
    if row.get("raw_value_json") is not None:
        return row.get("raw_value_json")
    return row.get("raw_value")


def _platform_outcome(row: Optional[Mapping[str, Any]]) -> str:
    if not row:
        return "MISSING"
    return str(
        row.get("eligibility_outcome")
        or row.get("normalized_result")
        or row.get("raw_result")
        or row.get("result")
        or "UNKNOWN"
    ).upper()


def _raw_check_name(row: Mapping[str, Any]) -> str:
    return _key(row.get("raw_name") or row.get("name") or row.get("normalized_name"))


def _normalized_check_name(row: Mapping[str, Any]) -> str:
    return _key(row.get("normalized_name") or row.get("raw_name") or row.get("name"))


def _check_rows_by_raw_name(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    severity = {"FAIL": 6, "WARNING": 5, "PENDING": 4, "UNKNOWN": 3, "PASS": 1, "NOT_APPLICABLE": 0}
    out: Dict[str, Dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        name = _raw_check_name(row)
        if not name:
            continue
        current = out.get(name)
        if current is None or severity.get(_platform_outcome(row), 2) > severity.get(_platform_outcome(current), 2):
            out[name] = row
    return out


def _first_check(by_raw: Mapping[str, Mapping[str, Any]], names: Sequence[str]) -> Optional[Dict[str, Any]]:
    wanted = [_key(x) for x in names if x]
    for name in wanted:
        row = by_raw.get(name)
        if row is not None:
            return dict(row)
    wanted_set = set(wanted)
    for row in by_raw.values():
        if _normalized_check_name(row) in wanted_set:
            return dict(row)
    return None


def _path_get(mapping: Mapping[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = mapping
    for part in str(path or "").split("."):
        if not part:
            continue
        if not isinstance(cur, Mapping) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _canonical_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def qualification_policy_hash(
    qualification_integration: Mapping[str, Any],
    classification_policy: Mapping[str, Any],
) -> str:
    """Hash only Qualification policy identity, never Simulation semantics."""
    return _canonical_hash({
        "qualification_integration": dict(qualification_integration or {}),
        "ppl_classification": dict(classification_policy or {}),
    })


def load_qualification_integration(project_dir: Path, filename: str = "ppl_round_v31.yaml") -> Dict[str, Any]:
    """Direct file loader used by tests/tools. Runtime code should use a snapshot."""
    path = Path(project_dir) / filename
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(raw.get("qualification_integration") or {})


def build_qualification_policy_snapshot(
    classification_policy: Mapping[str, Any], qualification_integration: Mapping[str, Any],
    *, source_path: str = "", mapped_version: Optional[str] = None,
) -> QualificationPolicySnapshot:
    """Build and validate one immutable Qualification policy snapshot.

    C3 uses this constructor both for a freshly-read YAML policy and for the
    durable active-policy payload restored after a process restart.  It has no
    database or network side effects.
    """
    classification = copy.deepcopy(dict(classification_policy or {}))
    integration = copy.deepcopy(dict(qualification_integration or {}))
    if integration.get("enabled", False):
        compile_rule_declarations(integration)
        declared_version = str(integration.get("policy_version") or "")
        if mapped_version is not None and declared_version != str(mapped_version or ""):
            raise ValueError(
                f"QUALIFICATION_POLICY_VERSION_MISMATCH:{declared_version}:{mapped_version or ''}"
            )
    return QualificationPolicySnapshot(
        classification_policy=classification,
        integration=integration,
        policy_hash=qualification_policy_hash(integration, classification),
        source_path=str(source_path or ""),
    )


def install_qualification_policy_runtime_snapshot(
    project_dir: Path, snapshot: QualificationPolicySnapshot, filename: str = "ppl_round_v31.yaml",
) -> QualificationPolicySnapshot:
    """Install a validated snapshot as the process-local active policy.

    Only the engine-side safe-checkpoint controller should call this in normal
    runtime.  Evaluation code remains read-only and simply consumes the cached
    snapshot.
    """
    path = (Path(project_dir) / filename).resolve()
    key = str(path)
    _RUNTIME_POLICY_SNAPSHOTS[key] = QualificationPolicySnapshot(
        classification_policy=copy.deepcopy(dict(snapshot.classification_policy or {})),
        integration=copy.deepcopy(dict(snapshot.integration or {})),
        policy_hash=str(snapshot.policy_hash),
        source_path=key,
    )
    return _RUNTIME_POLICY_SNAPSHOTS[key]


def load_qualification_policy_snapshot(
    project_dir: Path, filename: str = "ppl_round_v31.yaml", *, force_reload: bool = False,
) -> QualificationPolicySnapshot:
    """Return one process-local immutable Qualification snapshot.

    C2 freezes the policy after first use. C3 may replace that cached snapshot
    only at a durable safe checkpoint after validating version/hash transition.
    """
    path = (Path(project_dir) / filename).resolve()
    key = str(path)
    if not force_reload and key in _RUNTIME_POLICY_SNAPSHOTS:
        return _RUNTIME_POLICY_SNAPSHOTS[key]
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    integration = dict(raw.get("qualification_integration") or {})
    version_map = dict(raw.get("policy_versions") or {})
    snapshot = build_qualification_policy_snapshot(
        dict(raw.get("ppl_classification") or {}), integration,
        source_path=key, mapped_version=version_map.get("qualification"),
    )
    _RUNTIME_POLICY_SNAPSHOTS[key] = snapshot
    return snapshot


def clear_qualification_policy_runtime_cache() -> None:
    """Test/future-safe-checkpoint hook; never a remote side effect."""
    _RUNTIME_POLICY_SNAPSHOTS.clear()


def _parse_role(value: Any) -> RuleRole:
    try:
        return RuleRole(str(value))
    except ValueError as exc:
        raise ValueError(f"QUALIFICATION_RULE_ROLE_INVALID:{value}") from exc


def _parse_missing(value: Any) -> MissingFactAction:
    try:
        return MissingFactAction(str(value or MissingFactAction.UNRESOLVED.value))
    except ValueError as exc:
        raise ValueError(f"QUALIFICATION_RULE_ON_MISSING_INVALID:{value}") from exc


def compile_rule_declarations(integration: Mapping[str, Any]) -> Tuple[QualificationRuleDeclaration, ...]:
    declarations = []
    seen = set()
    for raw in integration.get("rules") or []:
        item = dict(raw or {})
        rule_id = _key(item.get("id"))
        if not rule_id or rule_id in seen:
            raise ValueError(f"QUALIFICATION_RULE_ID_INVALID_OR_DUPLICATE:{rule_id or '<missing>'}")
        seen.add(rule_id)
        declarations.append(QualificationRuleDeclaration(
            rule_id=rule_id,
            role=_parse_role(item.get("role")),
            source=_key(item.get("source")),
            fact=str(item.get("fact") or ""),
            operator=_key(item.get("operator")),
            threshold_from=str(item.get("threshold_from") or ""),
            names_from=str(item.get("names_from") or ""),
            name_from=str(item.get("name_from") or ""),
            aliases=tuple(str(x) for x in item.get("aliases") or ()),
            pass_outcomes=tuple(str(x).upper() for x in item.get("pass_outcomes") or ("PASS", "NOT_APPLICABLE")),
            on_missing=_parse_missing(item.get("on_missing")),
            failure_code=_key(item.get("failure_code")),
            repairable=bool(item.get("repairable", False)),
            effect=_key(item.get("effect")),
            metadata={k: v for k, v in item.items() if k not in {
                "id", "role", "source", "fact", "operator", "threshold_from", "names_from",
                "name_from", "aliases", "pass_outcomes", "on_missing", "failure_code", "repairable", "effect",
            }},
        ))
    return tuple(declarations)


def _missing_status(action: MissingFactAction) -> RuleEvaluationStatus:
    if action is MissingFactAction.NOT_APPLICABLE:
        return RuleEvaluationStatus.NOT_APPLICABLE
    if action is MissingFactAction.FAIL:
        return RuleEvaluationStatus.FAIL
    return RuleEvaluationStatus.UNRESOLVED


def _compare(actual: Any, expected: Any, operator: str) -> Optional[bool]:
    op = _key(operator)
    if op in {"GE", "GT", "LE", "LT"}:
        av, ev = _float(actual), _float(expected)
        if av is None or ev is None:
            return None
        if op == "GE":
            return av >= ev
        if op == "GT":
            return av > ev
        if op == "LE":
            return av <= ev
        return av < ev
    if op == "EQ":
        if actual is None:
            return None
        return str(actual).upper() == str(expected).upper()
    if op == "NE":
        if actual is None:
            return None
        return str(actual).upper() != str(expected).upper()
    raise ValueError(f"QUALIFICATION_RULE_OPERATOR_UNSUPPORTED:{operator}")


def _evaluation(
    decl: QualificationRuleDeclaration,
    *,
    status: RuleEvaluationStatus,
    actual: Any = None,
    expected: Any = None,
    platform_outcome: str = "",
    metadata: Optional[Mapping[str, Any]] = None,
) -> QualificationRuleEvaluation:
    return QualificationRuleEvaluation(
        rule_id=decl.rule_id,
        role=decl.role,
        status=status,
        actual=actual,
        expected=expected,
        failure_code=decl.failure_code if status is RuleEvaluationStatus.FAIL else "",
        repairable=bool(decl.repairable and status is RuleEvaluationStatus.FAIL),
        effect=decl.effect,
        source=decl.source,
        fact=decl.fact,
        platform_outcome=platform_outcome,
        metadata=dict(metadata or {}),
    )


def evaluate_declared_rules(
    candidate: Mapping[str, Any],
    metrics: Mapping[str, Any],
    check_rows: Iterable[Mapping[str, Any]],
    classification_policy: Mapping[str, Any],
    integration: Mapping[str, Any],
) -> Tuple[QualificationRuleEvaluation, ...]:
    """Evaluate declared current rules without composing a new PPL classification."""
    by_raw = _check_rows_by_raw_name(check_rows)
    results = []
    derived: Dict[str, QualificationRuleEvaluation] = {}

    for decl in compile_rule_declarations(integration):
        source = decl.source
        actual: Any = None
        expected: Any = None
        platform_outcome = ""
        metadata: Dict[str, Any] = {}

        if source == "METRIC":
            actual = metrics.get(decl.fact)
            expected = _path_get(classification_policy, decl.threshold_from) if decl.threshold_from else decl.metadata.get("expected")
            if actual is None:
                ev = _evaluation(decl, status=_missing_status(decl.on_missing), expected=expected)
            else:
                passed = _compare(actual, expected, decl.operator)
                ev = _evaluation(
                    decl,
                    status=RuleEvaluationStatus.PASS if passed else RuleEvaluationStatus.FAIL,
                    actual=_float(actual) if _float(actual) is not None else actual,
                    expected=_float(expected) if _float(expected) is not None else expected,
                )

        elif source == "CANDIDATE":
            actual = candidate.get(decl.fact)
            if actual is None and "default" in decl.metadata:
                actual = decl.metadata.get("default")
            expected = _path_get(classification_policy, decl.threshold_from) if decl.threshold_from else decl.metadata.get("expected")
            if actual is None:
                ev = _evaluation(decl, status=_missing_status(decl.on_missing), expected=expected)
            else:
                passed = _compare(actual, expected, decl.operator)
                ev = _evaluation(decl, status=RuleEvaluationStatus.PASS if passed else RuleEvaluationStatus.FAIL,
                                 actual=actual, expected=expected)

        elif source == "CHECK_OUTCOME":
            names = list(_path_get(classification_policy, decl.names_from, []) or []) if decl.names_from else []
            if decl.name_from:
                resolved = _path_get(classification_policy, decl.name_from)
                if resolved:
                    names.append(str(resolved))
            names.extend(decl.aliases)
            row = _first_check(by_raw, names)
            if row is None:
                ev = _evaluation(decl, status=_missing_status(decl.on_missing), metadata={"check_names": names})
            else:
                platform_outcome = _platform_outcome(row)
                passed = platform_outcome in set(decl.pass_outcomes)
                ev = _evaluation(
                    decl,
                    status=RuleEvaluationStatus.PASS if passed else RuleEvaluationStatus.FAIL,
                    actual=_raw_value(row), expected=list(decl.pass_outcomes), platform_outcome=platform_outcome,
                    metadata={"check": _raw_check_name(row), "check_names": names},
                )

        elif source == "CHECK_VALUE_BAND":
            names = list(_path_get(classification_policy, decl.names_from, []) or []) if decl.names_from else []
            names.extend(decl.aliases)
            row = _first_check(by_raw, names)
            value = _float(_raw_value(row or {}))
            clean = _float(_path_get(classification_policy, str(decl.metadata.get("clean_max_from") or "")))
            mid = _float(_path_get(classification_policy, str(decl.metadata.get("mid_max_from") or "")))
            if value is None or clean is None or mid is None:
                ev = _evaluation(decl, status=_missing_status(decl.on_missing), metadata={"band": "UNRESOLVED"})
            else:
                band = "CLEAN" if value <= clean else "MID" if value < mid else "HIGH"
                # A band classifier is informational; HIGH is represented as FAIL
                # because the current local strategy rejects that band.
                status = RuleEvaluationStatus.FAIL if band == "HIGH" else RuleEvaluationStatus.PASS
                ev = _evaluation(
                    decl, status=status, actual=value, expected={"clean_max": clean, "mid_max": mid},
                    platform_outcome=_platform_outcome(row), metadata={"band": band, "check": _raw_check_name(row or {})},
                )

        elif source == "DERIVED_RULE":
            depends_on = _key(decl.metadata.get("depends_on"))
            parent = derived.get(depends_on)
            when_value = _key(decl.metadata.get("when_metadata_value"))
            metadata_key = str(decl.metadata.get("metadata_key") or "band")
            if parent is None or _key(parent.metadata.get(metadata_key)) != when_value:
                ev = _evaluation(decl, status=RuleEvaluationStatus.NOT_APPLICABLE,
                                 metadata={"depends_on": depends_on})
            else:
                actual = metrics.get(decl.fact)
                expected = _path_get(classification_policy, decl.threshold_from)
                if actual is None:
                    ev = _evaluation(decl, status=_missing_status(decl.on_missing), expected=expected)
                else:
                    passed = _compare(actual, expected, decl.operator)
                    ev = _evaluation(decl, status=RuleEvaluationStatus.PASS if passed else RuleEvaluationStatus.FAIL,
                                     actual=_float(actual), expected=_float(expected), metadata={"depends_on": depends_on})

        elif source == "CHECK_PENDING_GROUP":
            names = list(_path_get(classification_policy, decl.names_from, []) or [])
            present = []
            for name in names:
                row = _first_check(by_raw, [name])
                if row is not None and _platform_outcome(row) not in set(decl.pass_outcomes):
                    present.append(_raw_check_name(row))
            if not present:
                ev = _evaluation(decl, status=RuleEvaluationStatus.NOT_APPLICABLE, metadata={"pending_checks": []})
            else:
                ev = _evaluation(decl, status=RuleEvaluationStatus.FAIL, actual=tuple(present),
                                 expected=list(decl.pass_outcomes), metadata={"pending_checks": present})

        elif source == "CHECK_DIAGNOSTIC_SET":
            names = list(_path_get(classification_policy, decl.names_from, []) or [])
            triggered = []
            for name in names:
                row = _first_check(by_raw, [name])
                if row is not None and _platform_outcome(row) not in set(decl.pass_outcomes):
                    triggered.append(_raw_check_name(row))
            status = RuleEvaluationStatus.FAIL if triggered else RuleEvaluationStatus.PASS
            ev = _evaluation(decl, status=status, actual=tuple(triggered),
                             expected=list(decl.pass_outcomes), metadata={"triggered_checks": triggered})

        else:
            raise ValueError(f"QUALIFICATION_RULE_SOURCE_UNSUPPORTED:{source}")

        results.append(ev)
        derived[decl.rule_id] = ev

    return tuple(results)


def _legacy_unresolved(legacy: Mapping[str, Any]) -> Tuple[str, ...]:
    values = [str(x) for x in legacy.get("fixed_unresolved") or ()]
    classification = str(legacy.get("classification") or "")
    if classification in {"PPL_CHECK_UNRESOLVED", "PPL_THEME_UNRESOLVED", "UNCLASSIFIED"}:
        values.extend(str(x) for x in legacy.get("reasons") or ())
    return tuple(dict.fromkeys(x for x in values if x))


def evaluate_ppl_qualification_compatibility(
    candidate: Mapping[str, Any],
    metrics: Mapping[str, Any],
    check_rows: Iterable[Mapping[str, Any]],
    classification_policy: Mapping[str, Any],
    integration: Mapping[str, Any],
    legacy_classification: Mapping[str, Any],
) -> QualificationEvaluationBundle:
    """Project current PPL behavior into the stable Qualification contract.

    C2 parity mode intentionally accepts the proven legacy classification as the
    final composer.  Declared rules are evaluated independently and attributed
    by role, so C3 can later replace the composer only after parity evidence is
    exhaustive.
    """
    rules = evaluate_declared_rules(candidate, metrics, check_rows, classification_policy, integration)
    policy_version = str(integration.get("policy_version") or "")
    classification = str(legacy_classification.get("classification") or "UNCLASSIFIED")

    repairable = tuple(str(x) for x in legacy_classification.get("repair_drivers") or () if x)
    terminal_or_strategy = []
    if classification == "PPL_TERMINAL_FAIL":
        terminal_or_strategy.extend(str(x) for x in legacy_classification.get("reasons") or ())
    blockers = tuple(dict.fromkeys([*terminal_or_strategy, *repairable]))
    unresolved = _legacy_unresolved(legacy_classification)

    diagnostic_names = []
    for item in legacy_classification.get("quality_diagnostics") or ():
        if item.get("check"):
            diagnostic_names.append(str(item["check"]))
    for item in legacy_classification.get("structural_diagnostics") or ():
        if item.get("check"):
            diagnostic_names.append(str(item["check"]))
    for item in legacy_classification.get("unmapped_theme_signals") or ():
        if item.get("check"):
            diagnostic_names.append(str(item["check"]))

    platform_facts: Dict[str, Any] = {
        "final_theme_check": legacy_classification.get("final_theme_check"),
        "final_theme_outcome": legacy_classification.get("final_theme_outcome"),
        "power_pool_correlation_outcome": legacy_classification.get("platform_ppc_outcome"),
    }
    for rule in rules:
        if rule.role is RuleRole.PLATFORM_HARD_RULE:
            platform_facts[rule.rule_id] = {
                "status": rule.status.value,
                "actual": rule.actual,
                "expected": rule.expected,
                "platform_outcome": rule.platform_outcome,
                "effect": rule.effect,
            }

    local_strategy_results: Dict[str, Any] = {
        "ppc_policy_band": legacy_classification.get("ppc_policy_band"),
        "ppc_strategy_result": legacy_classification.get("ppc_strategy_result"),
    }
    for rule in rules:
        if rule.role in {RuleRole.LOCAL_QUALIFICATION_RULE, RuleRole.LOCAL_STRATEGY_RULE}:
            local_strategy_results[rule.rule_id] = {
                "role": rule.role.value,
                "status": rule.status.value,
                "actual": rule.actual,
                "expected": rule.expected,
                "effect": rule.effect,
                **dict(rule.metadata or {}),
            }

    result = QualificationResult(
        classification=classification,
        qualified=(classification == "PPL_TECHNICALLY_READY"),
        blockers=blockers,
        unresolved=unresolved,
        diagnostics=tuple(dict.fromkeys(diagnostic_names)),
        repairable_failure_codes=repairable,
        platform_facts=platform_facts,
        local_strategy_results=local_strategy_results,
        policy_version=policy_version,
    )
    return QualificationEvaluationBundle(
        result=result,
        rules=rules,
        evaluator_version=QUALIFICATION_EVALUATOR_VERSION,
        policy_hash=qualification_policy_hash(integration, classification_policy),
    )
