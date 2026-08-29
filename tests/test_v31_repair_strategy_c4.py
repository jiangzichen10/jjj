from pathlib import Path
import ast
import inspect
import yaml

import ppl_engine.round_orchestrator as ro
from ppl_engine.repair_strategy import (
    direction_repair_value,
    rank_repair_candidates,
    repair_value,
)

ROOT = Path(__file__).resolve().parents[1]


def _policy():
    return yaml.safe_load((ROOT / 'ppl_round_v31.yaml').read_text(encoding='utf-8'))


def test_c4_repair_value_compatibility_export_matches_pure_strategy():
    p = _policy()
    for item in (
        {'sharpe': 3.2, 'turnover': 0.75},
        {'sharpe': 2.4, 'turnover': 0.90},
        {'sharpe': 1.2, 'turnover': 0.80},
    ):
        assert ro._repair_value(item, p) == repair_value(item, p)
    assert ro._direction_repair_value({'sharpe': -2.2, 'turnover': 0.4}, p) == direction_repair_value({'sharpe': -2.2, 'turnover': 0.4}, p)


def test_c4_repair_candidate_ranking_is_deterministic():
    p = _policy()
    items = [
        {'candidate_id':'b','repair_priority':'MEDIUM','max_normalized_gap':0.03,'sharpe':2.4,'turnover':0.75},
        {'candidate_id':'a','repair_priority':'HIGH','max_normalized_gap':0.04,'sharpe':3.2,'turnover':0.75},
        {'candidate_id':'c','repair_priority':'HIGH','max_normalized_gap':0.01,'sharpe':2.1,'turnover':1.2},
    ]
    a = [x['candidate_id'] for x in rank_repair_candidates(items, p)]
    b = [x['candidate_id'] for x in rank_repair_candidates(items, p)]
    assert a == b
    assert a[0] == 'a'


def test_c4_repair_strategy_module_has_no_durable_or_http_primitives():
    module = __import__('ppl_engine.repair_strategy', fromlist=['*'])
    tree = ast.parse(inspect.getsource(module))
    imported = set()
    called_attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(str(node.module or ''))
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            called_attrs.add(node.func.attr)
    assert 'sqlite3' not in imported
    assert 'requests' not in imported
    assert 'connect' not in called_attrs
    src = inspect.getsource(ro._select_repair_batch)
    assert 'rank_repair_candidates(' in src
