"""Read-only Dataset/Field discovery and auditable semantic classification."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd


SEMANTIC_CLASSES = (
    "RETURN", "VOLUME", "PRICE_LEVEL", "VOLATILITY", "SPREAD_COST",
    "FLOW", "SENTIMENT", "FUNDAMENTAL", "UNKNOWN",
)
FIELD_TYPES = ("MATRIX", "VECTOR")
CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW")


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _text(value: Any) -> str:
    if isinstance(value, Mapping):
        return " ".join(str(v) for v in value.values() if v is not None)
    return "" if value is None else str(value)


def _match_rules(text: str, rules: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    matches = []
    for rule in rules:
        for pattern in rule.get("patterns", []):
            found = re.search(str(pattern), text, flags=re.IGNORECASE)
            if found:
                matches.append({
                    "semantic_class": str(rule["semantic_class"]).upper(),
                    "rule_id": str(rule["rule_id"]),
                    "confidence": str(rule.get("confidence", "LOW")).upper(),
                    "matched_text": found.group(0),
                })
                break
    return matches


def classify_field(raw: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    """Classify one field without conflating storage type and economic meaning."""
    field_type = str(raw.get("type") or "").upper()
    if field_type not in FIELD_TYPES:
        field_type = str(raw.get("type") or "UNKNOWN").upper()

    stages = []
    description = _text(raw.get("description"))
    phrase_matches = []
    description_folded = description.casefold()
    for rule in rules.get("phrase_overrides", []):
        for phrase in rule.get("phrases", []):
            if str(phrase).casefold() in description_folded:
                phrase_matches.append({
                    "semantic_class": str(rule["semantic_class"]).upper(),
                    "rule_id": str(rule["rule_id"]),
                    "confidence": str(rule.get("confidence", "HIGH")).upper(),
                    "matched_text": str(phrase),
                })
                break
    stages.append(("PHRASE_OVERRIDE", phrase_matches))

    category_text = " ".join((_text(raw.get("category")), _text(raw.get("subcategory"))))
    stages.append(("CATEGORY", _match_rules(category_text, rules.get("category_rules", []))))
    stages.append(("FIELD_ID", _match_rules(_text(raw.get("id")), rules.get("field_id_rules", []))))
    stages.append(("DESCRIPTION", _match_rules(description, rules.get("description_rules", []))))

    for source, matches in stages:
        if not matches:
            continue
        classes = {item["semantic_class"] for item in matches}
        if len(classes) > 1:
            return {
                "field_type": field_type,
                "semantic_class": "UNKNOWN",
                "classification_source": source,
                "classification_rule_id": None,
                "classification_confidence": "LOW",
                "classification_warning": (
                    f"Conflicting {source} rules: "
                    + ", ".join(sorted(item["rule_id"] for item in matches))
                ),
                "matched_text": ", ".join(sorted({item["matched_text"] for item in matches})),
            }
        chosen = matches[0]
        confidence = chosen["confidence"] if chosen["confidence"] in CONFIDENCE_LEVELS else "LOW"
        return {
            "field_type": field_type,
            "semantic_class": chosen["semantic_class"],
            "classification_source": source,
            "classification_rule_id": chosen["rule_id"],
            "classification_confidence": confidence,
            "classification_warning": None,
            "matched_text": chosen["matched_text"],
        }

    return {
        "field_type": field_type,
        "semantic_class": "UNKNOWN",
        "classification_source": "FALLBACK_UNKNOWN",
        "classification_rule_id": None,
        "classification_confidence": "LOW",
        "classification_warning": "No configured semantic rule matched.",
        "matched_text": None,
    }


def _numeric(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def classify_dataset_hint(raw: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a coarse Dataset hint; Field semantics remain independently classified."""
    weights = rules["hint_weights"]
    confidence_multipliers = rules["confidence_multipliers"]
    sources = (
        ("ID_NAME", " ".join((_text(raw.get("id")), _text(raw.get("name")))), "HIGH"),
        ("CATEGORY", " ".join((_text(raw.get("category")), _text(raw.get("subcategory")))), "HIGH"),
        ("DESCRIPTION", _text(raw.get("description")), "LOW"),
    )
    matches = []
    for source_rank, (source, text, confidence) in enumerate(sources):
        for rule in rules.get("hint_rules", []):
            for pattern in rule.get("patterns", []):
                found = re.search(str(pattern), text, flags=re.IGNORECASE)
                if found:
                    hint = str(rule["hint"]).upper()
                    points = float(weights.get(hint, weights.get("UNKNOWN", 0))) * float(
                        confidence_multipliers.get(confidence, 0.6)
                    )
                    matches.append((points, -source_rank, hint, source, confidence, rule["rule_id"], found.group(0)))
                    break
    if not matches:
        return {
            "dataset_semantic_hint": "UNKNOWN",
            "dataset_hint_source": "FALLBACK_UNKNOWN",
            "dataset_hint_confidence": "LOW",
            "dataset_hint_rule_id": None,
            "dataset_hint_matched_text": None,
            "semantic_hint_points": round(
                float(weights.get("UNKNOWN", 0)) * float(confidence_multipliers.get("LOW", 0.6)), 4
            ),
        }
    chosen = max(matches)
    return {
        "dataset_semantic_hint": chosen[2],
        "dataset_hint_source": chosen[3],
        "dataset_hint_confidence": chosen[4],
        "dataset_hint_rule_id": chosen[5],
        "dataset_hint_matched_text": chosen[6],
        "semantic_hint_points": round(chosen[0], 4),
    }


def _threshold_points(value: Any, thresholds: Sequence[Sequence[float]]) -> float:
    number = _numeric(value)
    if number is None:
        return 0.0
    for upper, points in thresholds:
        if number <= float(upper):
            return float(points)
    return 0.0


def score_dataset_metadata(raw: Mapping[str, Any], hint: Mapping[str, Any], rules: Mapping[str, Any]) -> Dict[str, Any]:
    points = rules["metadata_points"]
    coverage = min(1.0, max(0.0, _numeric(raw.get("coverage")) or 0.0))
    coverage_points = coverage * float(points["coverage_max"])
    value_score = max(0.0, _numeric(raw.get("valueScore")) or 0.0)
    value_points = min(1.0, value_score / float(points["value_score_reference_max"])) * float(
        points["value_score_max"]
    )
    novelty_rules = points["novelty_thresholds"]
    novelty_points = _threshold_points(raw.get("alphaCount"), novelty_rules["alpha_count"])
    novelty_points += _threshold_points(raw.get("userCount"), novelty_rules["user_count"])
    novelty_points = min(float(points["novelty_max"]), novelty_points)
    components = {
        "semantic_hint": round(float(hint["semantic_hint_points"]), 4),
        "coverage_metadata": round(coverage_points + value_points, 4),
        "novelty": round(novelty_points, 4),
        "field_evidence": 0.0,
    }
    return {"score": round(sum(components.values()), 4), "components": components}


def score_field_evidence(fields: Sequence[Mapping[str, Any]], rules: Mapping[str, Any]) -> Dict[str, Any]:
    passing = [field for field in fields if field["coverage_pass"]]
    semantic_counts: Dict[str, int] = {}
    type_counts: Dict[str, int] = {}
    confidence_counts: Dict[str, int] = {}
    for field in passing:
        semantic_counts[field["semantic_class"]] = semantic_counts.get(field["semantic_class"], 0) + 1
        type_counts[field["field_type"]] = type_counts.get(field["field_type"], 0) + 1
        confidence_counts[field["classification_confidence"]] = confidence_counts.get(field["classification_confidence"], 0) + 1
    config = rules["field_evidence_points"]
    total = len(passing)
    semantic_weights = config["semantic_weights"]
    semantic_average = (
        sum(semantic_weights.get(field["semantic_class"], 0.0) for field in passing) / total
        if total else 0.0
    )
    confidence_weights = {"HIGH": 1.0, "MEDIUM": 0.6, "LOW": 0.2}
    confidence_average = (
        sum(confidence_weights.get(field["classification_confidence"], 0.0) for field in passing) / total
        if total else 0.0
    )
    diversity_ratio = min(1.0, len(type_counts) / 2.0) if total else 0.0
    components = {
        "semantic_relevance": round(semantic_average * float(config["semantic_relevance_max"]), 4),
        "coverage_depth": round(
            min(1.0, total / float(config["coverage_depth_reference"])) * float(config["coverage_depth_max"]), 4
        ),
        "field_type_diversity": round(diversity_ratio * float(config["field_type_diversity_max"]), 4),
        "classification_confidence": round(confidence_average * float(config["confidence_max"]), 4),
    }
    return {
        "score": round(sum(components.values()), 4),
        "components": components,
        "coverage_pass_fields": total,
        "semantic_class_counts": dict(sorted(semantic_counts.items())),
        "field_type_counts": dict(sorted(type_counts.items())),
        "classification_confidence_counts": dict(sorted(confidence_counts.items())),
    }


def _field_sort_key(field: Mapping[str, Any], route_priorities: Mapping[str, int]):
    confidence = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(field["classification_confidence"], 3)
    return (
        -float(field.get("coverage") or 0),
        route_priorities.get(field["semantic_class"], 9),
        confidence,
        int(field.get("alphaCount") or 0),
        int(field.get("userCount") or 0),
        field["field_id"],
    )


@dataclass
class DiscoveryResult:
    snapshot: Dict[str, Any]
    datasets: List[Dict[str, Any]]
    fields: List[Dict[str, Any]]


class ReadOnlySession:
    """Post-authentication firewall: Discovery can issue GET only.

    The wrapper exposes only the mutable requests.Session state required by
    ``machine_lib_V2_1.ensure_session`` during a 401/403 re-login. HTTP verb
    methods remain guarded here, so refreshing authentication cannot weaken the
    GET-only discovery firewall.
    """

    _AUTH_REFRESH_STATE_ATTRS = (
        "auth", "headers", "cookies", "trust_env", "verify", "cert",
        "max_redirects", "params", "proxies",
    )

    def __init__(self, session: Any):
        self._session = session
        self.methods: List[str] = []
        self.urls: List[str] = []
        self.status_counts: Dict[str, int] = {}

    # ``machine_lib_V2_1._copy_session_state`` writes these attributes after
    # a 401/403. Proxy them explicitly instead of using ``__getattr__`` so
    # unsupported HTTP verbs (for example PUT) cannot leak through.
    @property
    def auth(self):
        return self._session.auth

    @auth.setter
    def auth(self, value: Any) -> None:
        self._session.auth = value

    @property
    def headers(self):
        return self._session.headers

    @property
    def cookies(self):
        return self._session.cookies

    @property
    def trust_env(self):
        return self._session.trust_env

    @trust_env.setter
    def trust_env(self, value: Any) -> None:
        self._session.trust_env = value

    @property
    def verify(self):
        return self._session.verify

    @verify.setter
    def verify(self, value: Any) -> None:
        self._session.verify = value

    @property
    def cert(self):
        return self._session.cert

    @cert.setter
    def cert(self, value: Any) -> None:
        self._session.cert = value

    @property
    def max_redirects(self):
        return self._session.max_redirects

    @max_redirects.setter
    def max_redirects(self, value: Any) -> None:
        self._session.max_redirects = value

    @property
    def params(self):
        return self._session.params

    @params.setter
    def params(self, value: Any) -> None:
        self._session.params = value

    @property
    def proxies(self):
        return self._session.proxies

    @proxies.setter
    def proxies(self, value: Any) -> None:
        self._session.proxies = value

    def assert_auth_refresh_compatible(self) -> None:
        """Fail before round POSTs if the auth-refresh adapter contract regresses."""
        missing = []
        for name in self._AUTH_REFRESH_STATE_ATTRS:
            try:
                getattr(self, name)
            except AttributeError:
                missing.append(name)
        if missing:
            raise RuntimeError(
                "READ_ONLY_AUTH_REFRESH_INCOMPATIBLE: missing session state "
                + ",".join(missing)
            )

    def request(self, method: str, url: str, **kwargs: Any):
        method = str(method).upper()
        if method != "GET":
            raise RuntimeError(f"PHASE_2_READ_ONLY_VIOLATION: {method} {url}")
        self.methods.append(method)
        self.urls.append(str(url))
        response = self._session.request(method, url, **kwargs)
        status = str(getattr(response, "status_code", "UNKNOWN"))
        self.status_counts[status] = self.status_counts.get(status, 0) + 1
        return response

    def get(self, url: str, **kwargs: Any):
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any):
        raise RuntimeError(f"PHASE_2_READ_ONLY_VIOLATION: POST {url}")

    def patch(self, url: str, **kwargs: Any):
        raise RuntimeError(f"PHASE_2_READ_ONLY_VIOLATION: PATCH {url}")

    def delete(self, url: str, **kwargs: Any):
        raise RuntimeError(f"PHASE_2_READ_ONLY_VIOLATION: DELETE {url}")


def discover_online(session: Any, config: Any, machine_lib: Any) -> DiscoveryResult:
    settings = config.plan["simulation_settings"]
    selection = config.plan["selection"]
    discovery_rules = config.rules["discovery"]
    semantic_rules = config.rules["semantic_classification"]
    routes = config.rules["candidate_routes"]
    preselection_rules = config.rules["heuristics"]["dataset_preselection"]
    priority_value = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    route_priorities = {name: priority_value.get(route.get("priority"), 9) for name, route in routes.items()}

    datasets_df = machine_lib.get_datasets(
        session,
        instrument_type=settings["instrument_type"],
        region=settings["region"],
        delay=settings["delay"],
        universe=settings["universe"],
    )
    raw_datasets = [_json_safe(row) for row in datasets_df.to_dict(orient="records")]
    excluded_ids = set(config.rules["current_theme"].get("excluded_datasets", []))
    excluded_ids.update(selection.get("excluded_dataset_ids", []))
    available_ids = {str(row.get("id")) for row in raw_datasets if row.get("id") is not None}
    exclusion_status = {
        dataset_id: ("MATCHED" if dataset_id in available_ids else "EXCLUSION_UNMATCHED_WARNING")
        for dataset_id in sorted(excluded_ids)
    }

    datasets = []
    for raw in raw_datasets:
        dataset_id = str(raw.get("id"))
        hint = classify_dataset_hint(raw, preselection_rules)
        metadata_score = score_dataset_metadata(raw, hint, preselection_rules)
        datasets.append({
            "dataset_id": dataset_id,
            "selected": False,
            "in_discovery_pool": False,
            "excluded": dataset_id in excluded_ids,
            **hint,
            "dataset_preselection_score": metadata_score["score"],
            "preselection_components": metadata_score["components"],
            "field_evidence": None,
            "raw_metadata": raw,
        })

    explicit = [str(x) for x in selection.get("dataset_ids", [])]
    if explicit:
        discovery_pool_ids = [x for x in explicit if x in available_ids and x not in excluded_ids]
        automatic_preselection = False
    else:
        eligible = [item for item in datasets if not item["excluded"]]
        eligible.sort(key=lambda item: (-item["dataset_preselection_score"], item["dataset_id"]))
        pool_size = int(preselection_rules["max_dataset_discovery_pool"])
        discovery_pool_ids = [item["dataset_id"] for item in eligible[:pool_size]]
        automatic_preselection = True
    for item in datasets:
        item["in_discovery_pool"] = item["dataset_id"] in discovery_pool_ids

    all_fields: List[Dict[str, Any]] = []
    threshold = float(discovery_rules["min_data_coverage"])
    allowed_types = {str(x).upper() for x in discovery_rules["allowed_field_types"]}
    max_fields = int(selection.get("max_fields_per_dataset", 10**9))
    for dataset_id in discovery_pool_ids:
        frame = machine_lib.get_datafields(
            session,
            instrument_type=settings["instrument_type"],
            region=settings["region"],
            delay=settings["delay"],
            universe=settings["universe"],
            dataset_id=dataset_id,
        )
        raw_rows = [_json_safe(row) for row in frame.to_dict(orient="records")]
        # V2.2 treats dateCoverage as informational only. It is deliberately
        # excluded from the automatic Data Coverage gate.
        coverage_column = next(
            (name for name in ("dataCoverage", "coverage") if name in frame.columns),
            None,
        )
        dataset_fields = []
        for raw in raw_rows:
            classified = classify_field(raw, semantic_rules)
            coverage = _numeric(raw.get(coverage_column)) if coverage_column else None
            field_id = str(raw.get("id"))
            field_type = classified["field_type"]
            coverage_pass = coverage is not None and coverage >= threshold and field_type in allowed_types
            dataset_obj = raw.get("dataset") if isinstance(raw.get("dataset"), Mapping) else {}
            record = {
                "dataset_id": str(dataset_obj.get("id") or dataset_id),
                "field_id": field_id,
                "description": raw.get("description"),
                "field_type": field_type,
                "coverage": coverage,
                "dateCoverage": _numeric(raw.get("dateCoverage")),
                "userCount": raw.get("userCount"),
                "alphaCount": raw.get("alphaCount"),
                "pyramidMultiplier": _numeric(raw.get("pyramidMultiplier")),
                "category": raw.get("category"),
                "subcategory": raw.get("subcategory"),
                "themes": raw.get("themes"),
                **classified,
                "coverage_pass": coverage_pass,
                "selected": False,
                "raw_metadata": raw,
            }
            dataset_fields.append(record)
        all_fields.extend(dataset_fields)

    fields_by_dataset: Dict[str, List[Dict[str, Any]]] = {}
    for item in all_fields:
        fields_by_dataset.setdefault(item["dataset_id"], []).append(item)
    for item in datasets:
        if not item["in_discovery_pool"]:
            continue
        evidence = score_field_evidence(fields_by_dataset.get(item["dataset_id"], []), preselection_rules)
        item["field_evidence"] = evidence
        item["preselection_components"]["field_evidence"] = evidence["score"]
        item["dataset_preselection_score"] = round(sum(item["preselection_components"].values()), 4)

    if automatic_preselection:
        pool_records = [item for item in datasets if item["in_discovery_pool"]]
        pool_records.sort(key=lambda item: (-item["dataset_preselection_score"], item["dataset_id"]))
        final_ids = [item["dataset_id"] for item in pool_records[: int(selection["max_datasets"])]]
    else:
        final_ids = list(discovery_pool_ids)
    for item in datasets:
        item["selected"] = item["dataset_id"] in final_ids

    for dataset_id in final_ids:
        eligible_fields = [field for field in fields_by_dataset.get(dataset_id, []) if field["coverage_pass"]]
        eligible_fields.sort(key=lambda x: _field_sort_key(x, route_priorities))
        selected_field_ids = {field["field_id"] for field in eligible_fields[:max_fields]}
        for field in fields_by_dataset.get(dataset_id, []):
            field["selected"] = field["field_id"] in selected_field_ids

    created_at = datetime.now(timezone.utc).isoformat()
    metadata_material = {"datasets": datasets, "fields": all_fields}
    metadata_hash = hashlib.sha256(
        json.dumps(metadata_material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    snapshot = {
        "snapshot_id": f"disc_{stamp}_{metadata_hash[:10]}",
        "region": settings["region"],
        "universe": settings["universe"],
        "delay": settings["delay"],
        "instrument_type": settings["instrument_type"],
        "source": "BRAIN_API_READ_ONLY",
        "dataset_count": len(datasets),
        "field_count": len(all_fields),
        "metadata_hash": metadata_hash,
        "exclusion_status": exclusion_status,
        "automatic_preselection": automatic_preselection,
        "discovery_pool_size": len(discovery_pool_ids),
        "created_at": created_at,
    }
    return DiscoveryResult(snapshot=snapshot, datasets=datasets, fields=all_fields)


def rolling_probe_dataset_ids(
    raw_datasets: Sequence[Mapping[str, Any]],
    config: Any,
    *,
    excluded_dataset_ids: Sequence[str],
    probe_count: int,
) -> List[str]:
    """Pure ranking helper used by blocking and Continuous discovery paths.

    This function performs no HTTP and does not mutate durable state.  Keeping
    probe selection here ensures the V3.1 durable discovery queue fetches the
    exact same Dataset field pages that the legacy rolling-discovery algorithm
    would have selected.
    """
    probe_count = max(0, int(probe_count))
    if probe_count <= 0:
        return []
    selection = config.plan["selection"]
    preselection_rules = config.rules["heuristics"]["dataset_preselection"]
    hard_excluded = set(config.rules["current_theme"].get("excluded_datasets", []))
    hard_excluded.update(selection.get("excluded_dataset_ids", []))
    seen = {str(x) for x in excluded_dataset_ids if x is not None}
    ranked: List[Dict[str, Any]] = []
    for raw0 in raw_datasets:
        raw = _json_safe(raw0)
        dataset_id = str(raw.get("id"))
        if not dataset_id or dataset_id == "None" or dataset_id in hard_excluded or dataset_id in seen:
            continue
        hint = classify_dataset_hint(raw, preselection_rules)
        metadata_score = score_dataset_metadata(raw, hint, preselection_rules)
        ranked.append({
            "dataset_id": dataset_id,
            "dataset_preselection_score": metadata_score["score"],
        })
    ranked.sort(key=lambda item: (-float(item["dataset_preselection_score"]), item["dataset_id"]))
    return [str(item["dataset_id"]) for item in ranked[:probe_count]]


def discover_rolling_online(
    session: Any,
    config: Any,
    machine_lib: Any,
    *,
    excluded_dataset_ids: Sequence[str],
    probe_count: int,
    admit_count: int,
) -> DiscoveryResult:
    """Read-only incremental Dataset refresh for a running V3 round.

    Unlike :func:`discover_online`, this function does not rebuild the existing
    candidate universe. It performs one Dataset metadata GET, probes DataFields
    only for a small set of previously unseen Dataset IDs, and selects the best
    usable subset for additive admission. Existing Dataset/Field/Candidate facts
    are left untouched by the caller.

    `excluded_dataset_ids` is normally the set of Dataset IDs already admitted
    to the round (ACTIVE or COOLDOWN). This deliberately prevents a rolling
    refresh from creating duplicate candidate families for a Dataset that the
    round has already explored.
    """
    probe_count = max(0, int(probe_count))
    admit_count = max(0, min(int(admit_count), probe_count))
    if probe_count == 0 or admit_count == 0:
        now = datetime.now(timezone.utc).isoformat()
        return DiscoveryResult(
            snapshot={
                "snapshot_id": f"disc_roll_empty_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
                "region": config.plan["simulation_settings"]["region"],
                "universe": config.plan["simulation_settings"]["universe"],
                "delay": config.plan["simulation_settings"]["delay"],
                "instrument_type": config.plan["simulation_settings"]["instrument_type"],
                "source": "BRAIN_API_ROLLING_READ_ONLY",
                "dataset_count": 0,
                "field_count": 0,
                "metadata_hash": hashlib.sha256(b"[]").hexdigest(),
                "exclusion_status": {},
                "automatic_preselection": True,
                "discovery_pool_size": 0,
                "created_at": now,
                "rolling_admitted_dataset_ids": [],
                "rolling_probe_dataset_ids": [],
            },
            datasets=[],
            fields=[],
        )

    settings = config.plan["simulation_settings"]
    selection = config.plan["selection"]
    discovery_rules = config.rules["discovery"]
    semantic_rules = config.rules["semantic_classification"]
    routes = config.rules["candidate_routes"]
    preselection_rules = config.rules["heuristics"]["dataset_preselection"]
    priority_value = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    route_priorities = {name: priority_value.get(route.get("priority"), 9) for name, route in routes.items()}

    datasets_df = machine_lib.get_datasets(
        session,
        instrument_type=settings["instrument_type"],
        region=settings["region"],
        delay=settings["delay"],
        universe=settings["universe"],
    )
    raw_datasets = [_json_safe(row) for row in datasets_df.to_dict(orient="records")]
    hard_excluded = set(config.rules["current_theme"].get("excluded_datasets", []))
    hard_excluded.update(selection.get("excluded_dataset_ids", []))
    seen = {str(x) for x in excluded_dataset_ids if x is not None}
    available_ids = {str(row.get("id")) for row in raw_datasets if row.get("id") is not None}
    exclusion_status = {
        dataset_id: ("MATCHED" if dataset_id in available_ids else "EXCLUSION_UNMATCHED_WARNING")
        for dataset_id in sorted(hard_excluded)
    }

    probe_ids = rolling_probe_dataset_ids(
        raw_datasets, config, excluded_dataset_ids=sorted(seen), probe_count=probe_count
    )
    probe_set = set(probe_ids)
    ranked: List[Dict[str, Any]] = []
    for raw in raw_datasets:
        dataset_id = str(raw.get("id"))
        if dataset_id in hard_excluded or dataset_id in seen or dataset_id not in probe_set:
            continue
        hint = classify_dataset_hint(raw, preselection_rules)
        metadata_score = score_dataset_metadata(raw, hint, preselection_rules)
        ranked.append({
            "dataset_id": dataset_id,
            "selected": False,
            "in_discovery_pool": True,
            "excluded": False,
            **hint,
            "dataset_preselection_score": metadata_score["score"],
            "preselection_components": metadata_score["components"],
            "field_evidence": None,
            "raw_metadata": raw,
        })
    ranked.sort(key=lambda item: probe_ids.index(item["dataset_id"]) if item["dataset_id"] in probe_set else 10**9)
    probed = ranked

    all_fields: List[Dict[str, Any]] = []
    threshold = float(discovery_rules["min_data_coverage"])
    allowed_types = {str(x).upper() for x in discovery_rules["allowed_field_types"]}
    max_fields = int(selection.get("max_fields_per_dataset", 10**9))
    for dataset_id in probe_ids:
        frame = machine_lib.get_datafields(
            session,
            instrument_type=settings["instrument_type"],
            region=settings["region"],
            delay=settings["delay"],
            universe=settings["universe"],
            dataset_id=dataset_id,
        )
        raw_rows = [_json_safe(row) for row in frame.to_dict(orient="records")]
        coverage_column = next((name for name in ("dataCoverage", "coverage") if name in frame.columns), None)
        for raw in raw_rows:
            classified = classify_field(raw, semantic_rules)
            coverage = _numeric(raw.get(coverage_column)) if coverage_column else None
            field_id = str(raw.get("id"))
            field_type = classified["field_type"]
            coverage_pass = coverage is not None and coverage >= threshold and field_type in allowed_types
            dataset_obj = raw.get("dataset") if isinstance(raw.get("dataset"), Mapping) else {}
            all_fields.append({
                "dataset_id": str(dataset_obj.get("id") or dataset_id),
                "field_id": field_id,
                "description": raw.get("description"),
                "field_type": field_type,
                "coverage": coverage,
                "dateCoverage": _numeric(raw.get("dateCoverage")),
                "userCount": raw.get("userCount"),
                "alphaCount": raw.get("alphaCount"),
                "pyramidMultiplier": _numeric(raw.get("pyramidMultiplier")),
                "category": raw.get("category"),
                "subcategory": raw.get("subcategory"),
                "themes": raw.get("themes"),
                **classified,
                "coverage_pass": coverage_pass,
                "selected": False,
                "raw_metadata": raw,
            })

    fields_by_dataset: Dict[str, List[Dict[str, Any]]] = {}
    for item in all_fields:
        fields_by_dataset.setdefault(item["dataset_id"], []).append(item)
    usable: List[Dict[str, Any]] = []
    for item in probed:
        evidence = score_field_evidence(fields_by_dataset.get(item["dataset_id"], []), preselection_rules)
        item["field_evidence"] = evidence
        item["preselection_components"]["field_evidence"] = evidence["score"]
        item["dataset_preselection_score"] = round(sum(item["preselection_components"].values()), 4)
        if int(evidence.get("coverage_pass_fields") or 0) > 0:
            usable.append(item)
    usable.sort(key=lambda item: (-float(item["dataset_preselection_score"]), item["dataset_id"]))
    admitted_ids = [item["dataset_id"] for item in usable[:admit_count]]
    for item in probed:
        item["selected"] = item["dataset_id"] in admitted_ids

    for dataset_id in admitted_ids:
        eligible_fields = [field for field in fields_by_dataset.get(dataset_id, []) if field["coverage_pass"]]
        eligible_fields.sort(key=lambda x: _field_sort_key(x, route_priorities))
        selected_field_ids = {field["field_id"] for field in eligible_fields[:max_fields]}
        for field in fields_by_dataset.get(dataset_id, []):
            field["selected"] = field["field_id"] in selected_field_ids

    created_at = datetime.now(timezone.utc).isoformat()
    metadata_material = {"datasets": probed, "fields": all_fields, "seen": sorted(seen)}
    metadata_hash = hashlib.sha256(
        json.dumps(metadata_material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    snapshot = {
        "snapshot_id": f"disc_roll_{stamp}_{metadata_hash[:10]}",
        "region": settings["region"],
        "universe": settings["universe"],
        "delay": settings["delay"],
        "instrument_type": settings["instrument_type"],
        "source": "BRAIN_API_ROLLING_READ_ONLY",
        "dataset_count": len(probed),
        "field_count": len(all_fields),
        "metadata_hash": metadata_hash,
        "exclusion_status": exclusion_status,
        "automatic_preselection": True,
        "discovery_pool_size": len(probe_ids),
        "created_at": created_at,
        "rolling_admitted_dataset_ids": admitted_ids,
        "rolling_probe_dataset_ids": probe_ids,
        "rolling_seen_dataset_count": len(seen),
    }
    return DiscoveryResult(snapshot=snapshot, datasets=probed, fields=all_fields)
