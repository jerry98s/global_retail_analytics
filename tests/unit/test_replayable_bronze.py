"""Regression coverage for lossless ingestion and downstream validation."""
import importlib
import json

import pytest

pytestmark = pytest.mark.unit


def parser():
    try:
        module = importlib.import_module('spark.identity_resolution.event_contract')
    except ModuleNotFoundError:
        pytest.fail('Silver replay parser is missing')
    return module.parse_event


def click(**changes):
    value = dict(event_id='E1', event_type='page_view',
                 event_time='2026-09-17T12:00:00+08:00', session_id='s1',
                 client_id='c1', customer_id=None, platform='web',
                 app_version='1.2.0', schema_version='1.2.0', properties={})
    value.update(changes)
    return json.dumps(value).encode()


@pytest.mark.parametrize('payload,reason', [
    (None, 'tombstone'), (b'\xff', 'invalid_utf8'),
    (b'{oops', 'invalid_json'), (b'[]', 'invalid_envelope'),
    (b'{"event_id":"a","event_id":"b"}', 'invalid_json'),
    (b'{"properties":{"value":1e400}}', 'invalid_json'),
])
def test_invalid_records_have_explicit_quarantine_reason(payload, reason):
    assert parser()(payload, 'clickstream')['error_reason'] == reason


def test_old_events_are_valid_and_normalized_to_utc_without_lateness_filter():
    result = parser()(click(event_time='2001-01-01T12:00:00+08:00'), 'clickstream')
    assert result['error_reason'] is None
    assert result['event_time'] == '2001-01-01T04:00:00.000000'


@pytest.mark.parametrize('changes,reason', [
    ({'event_time': 'not-a-time'}, 'invalid_event_time'),
    ({'client_id': ' '}, 'missing_client_id'),
    ({'properties': 'not-an-object'}, 'invalid_properties'),
    ({'event_type': 'checkout', 'properties': {'order_id': 'ORDER-ABCDEF123456', 'cart_value': -1}}, 'checkout_invalid_cart_value'),
    ({'event_type': 'checkout', 'properties': {'order_id': 'ORDER-ABCDEF123456', 'cart_value': 'oops'}}, 'checkout_invalid_cart_value'),
    ({'event_type': 'checkout', 'properties': {'order_id': 'ORDER-ABCDEF123456', 'cart_value': 10**400}}, 'checkout_invalid_cart_value'),
])
def test_invalid_business_data_does_not_enter_canonical_events(changes, reason):
    assert parser()(click(**changes), 'clickstream')['error_reason'] == reason


def test_inventory_requires_integer_delta():
    payload = json.dumps(dict(event_id='I1', event_time='2026-09-17T00:00:00Z',
        store_id='S1', product_id='P1', qty_delta=True, event_type='receipt',
        scanner_id='D1', schema_version='1.0.0')).encode()
    assert parser()(payload, 'inventory')['error_reason'] == 'invalid_qty_delta'


@pytest.mark.parametrize('stream', ['clickstream', 'inventory'])
def test_raw_capture_sql_does_not_transform_or_discard_records(stream):
    try:
        module = importlib.import_module('streaming.flink_jobs.raw_bronze')
    except ModuleNotFoundError:
        pytest.fail('Raw Kafka capture implementation is missing')
    source, sink, insert = module.capture_sql({
        'kafka_bootstrap_servers': 'broker:9092', 'clickstream_topic': 'clicks',
        'inventory_topic': 'inventory', 'consumer_group_inventory_bronze': 'capture-inventory',
        'consumer_group_clickstream': 'capture', 'bronze_source_system': 'retail-v1',
    }, stream, 'iceberg')
    assert "'value.format' = 'raw'" in source
    assert "METADATA FROM 'offset'" in source
    assert 'raw_payload BYTES' in source and 'raw_key BYTES' in source
    assert 'WATERMARK' not in source
    assert 'PARTITIONED BY (ingestion_date)' in sink
    assert 'raw_payload' in insert and 'kafka_offset' in insert
    assert all(token not in insert.upper() for token in ('WHERE', 'ROW_NUMBER', 'DISTINCT', 'GROUP BY'))


@pytest.mark.parametrize('stream', ['clickstream', 'inventory'])
def test_bronze_entrypoints_delegate_to_raw_capture(stream, monkeypatch):
    module = importlib.import_module(f'streaming.flink_jobs.{stream}_bronze_job')
    called = []
    monkeypatch.setattr(module, 'run_capture', called.append)
    module.run()
    assert called == [stream]


def test_replay_enums_match_ingestion_schema_contracts():
    from spark.identity_resolution.event_contract import CLICKSTREAM_TYPES, INVENTORY_TYPES
    from streaming.flink_jobs.event_types import CLICKSTREAM_EVENT_TYPES, INVENTORY_EVENT_TYPES

    assert CLICKSTREAM_TYPES == set(CLICKSTREAM_EVENT_TYPES)
    assert INVENTORY_TYPES == set(INVENTORY_EVENT_TYPES)


def test_cloud_flink_step_quotes_and_forwards_source_incarnation():
    import ast
    import shlex
    from pathlib import Path

    from jinja2 import Environment

    path = Path(__file__).resolve().parents[2] / 'orchestration/airflow/dags/streaming_manual_flink_jobs.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_flink_step')
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    dag = next(node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'DAG')
    filter_kw = next((kw for kw in dag.keywords if kw.arg == 'user_defined_filters'), None)
    assert filter_kw is not None, 'DAG must register its shell quoting filter'
    filters = eval(compile(ast.Expression(filter_kw.value), str(path), 'eval'), {'shlex': shlex})
    environment = Environment()
    environment.filters.update(filters)
    template = namespace['_flink_step']('clicks', 'clickstream_bronze_job.py')['HadoopJarStep']['Args'][2]
    # Shell metacharacters remain one literal argument, never executable syntax.
    source_id = "cluster-v2';$(not-a-command)"
    rendered = environment.from_string(template).render(var={'value': {'bronze_source_system': source_id}})
    tokens = shlex.split(rendered)
    assert 'BRONZE_SOURCE_SYSTEM=' + source_id in tokens
    script = tokens.index('/opt/flink-config-src/bin/submit_flink_job.sh')
    assert tokens[script + 1:script + 3] == ['clicks', 'clickstream_bronze_job.py']


def test_submit_script_forwards_every_config_variable_to_yarn_containers():
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    script = (root / 'streaming/bin/submit_flink_job.sh').read_text(encoding='utf-8')
    conf = '\n'.join(
        line for line in (root / 'streaming/config/flink_conf.yaml').read_text(encoding='utf-8').splitlines()
        if not line.lstrip().startswith('#')
    )
    jobs = ''.join(p.read_text(encoding='utf-8') for p in (root / 'streaming/flink_jobs').glob('*.py'))
    read_vars = set(re.findall(r'\$\{([A-Z_]+)', conf)) | set(re.findall(r'os\.environ\.get\("([A-Z_]+)"', jobs))
    forwarded = set(re.search(r'forwarded=\(([^)]*)\)', script).group(1).split())
    assert read_vars <= forwarded, f'not forwarded to the JobManager: {sorted(read_vars - forwarded)}'
    assert '-Dcontainerized.master.env.${var}' in script
