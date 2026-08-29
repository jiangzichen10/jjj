from pathlib import Path
import inspect

from ppl_engine.search_strategy import adaptive_scores, select_search_candidates
import ppl_engine.round_orchestrator as ro

ROOT = Path(__file__).resolve().parents[1]


def _policy():
    import yaml
    return yaml.safe_load((ROOT / 'ppl_round_v31.yaml').read_text(encoding='utf-8'))


def test_c4_orchestrator_compat_export_delegates_to_pure_search_strategy():
    policy = _policy()
    evidence = {
        'dataset': {'d': {'attempts': 3, 'search_viable': 1}},
        'operator': {'raw': {'attempts': 3, 'search_viable': 1}},
        'dataset_operator': {('d','raw'): {'attempts': 3, 'search_viable': 1}},
        'operator_window': {('raw','NONE'): {'attempts': 3, 'search_viable': 1}},
        'dataset_operator_window': {('d','raw','NONE'): {'attempts': 3, 'search_viable': 1}},
        'dataset_operator_window_batch': {},
    }
    row = {'dataset_id':'d','operator':'raw','window':None,'initial_selection_score':100.0}
    assert ro._adaptive_scores(row, evidence, policy) == adaptive_scores(row, evidence, policy)


def test_c4_pure_search_selector_is_deterministic_and_respects_diversity():
    policy = _policy()
    policy = dict(policy)
    policy['batch_size'] = 4
    policy['exploration_fraction'] = 0.25
    initial_rules = {
        'max_dataset_fraction': 0.50,
        'max_semantic_class_fraction': 1.0,
        'max_initial_candidates_per_field': 1,
    }
    rows = []
    for i, (ds, field, score, eligible) in enumerate([
        ('d1','f1',10,True), ('d1','f1',9,True), ('d1','f2',8,True),
        ('d2','f3',7,True), ('d3','f4',6,False),
    ], 1):
        rows.append({
            'candidate_id': f'c{i}', 'dataset_id': ds, 'field_id': field,
            'semantic_class': 'PRICE', 'operator': 'raw',
            '_strategy_family_id': f'fam{i}', '_strategy_requires_new_post': True,
            'round_cache_action': 'NEW_SIMULATION_REQUIRED',
            'round_exploit_score': float(score), 'round_explore_score': float(score),
            'round_exploit_eligible': eligible,
        })
    a = select_search_candidates(
        rows, protected_families=(), active_datasets=('d1','d2','d3'),
        attempted_families=(), initial_rules=initial_rules, policy=policy,
        remaining=4, extension_batch_cap=2,
    )
    b = select_search_candidates(
        rows, protected_families=(), active_datasets=('d1','d2','d3'),
        attempted_families=(), initial_rules=initial_rules, policy=policy,
        remaining=4, extension_batch_cap=2,
    )
    assert [x['candidate_id'] for x in a.selected] == [x['candidate_id'] for x in b.selected]
    # Same dataset+field cannot consume two slots under the immutable diversity envelope.
    chosen = [(x['dataset_id'], x['field_id']) for x in a.selected]
    assert len(chosen) == len(set(chosen))


def test_c4_search_strategy_module_has_no_durable_or_http_primitives():
    import ast
    module = __import__('ppl_engine.search_strategy', fromlist=['*'])
    src = inspect.getsource(module)
    tree = ast.parse(src)
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
    assert not any(name.endswith('.store') or name == 'ppl_engine.store' for name in imported)
    assert 'requests' not in imported
    assert 'connect' not in called_attrs
    orchestration = inspect.getsource(ro._select_search_batch)
    assert 'select_search_candidates(' in orchestration
