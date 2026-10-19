"""Replay-time event validation. Original bytes always remain in Bronze.

Schema/business interpretation belongs here, never in the raw capture sink.
Returns JSON-compatible typed fields for the Spark worker UDF.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone

CLICKSTREAM_TYPES = {'page_view', 'product_view', 'search', 'add_to_cart',
                    'remove_from_cart', 'checkout_start', 'checkout', 'login', 'logout', 'error'}
INVENTORY_TYPES = {'sale_deduction', 'receipt', 'adjustment', 'transfer'}


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError(f'Non-finite JSON number: {value}')


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError('JSON number exceeds finite range')
    return number


def parse_event(payload: bytes | None, stream: str) -> dict:
    if stream not in {'clickstream', 'inventory'}:
        raise ValueError(f'Unknown stream: {stream}')

    def invalid(reason):
        return {'error_reason': reason}

    if payload is None:
        return invalid('tombstone')
    try:
        text = bytes(payload).decode('utf-8', errors='strict')
    except UnicodeDecodeError:
        return invalid('invalid_utf8')
    try:
        event = json.loads(text, object_pairs_hook=_object, parse_constant=_reject_constant,
                           parse_float=_finite_float)
    except (ValueError, RecursionError):
        return invalid('invalid_json')
    if not isinstance(event, dict):
        return invalid('invalid_envelope')

    required = ['event_id', 'event_type', 'schema_version']
    required += ['session_id', 'client_id', 'platform', 'app_version'] if stream == 'clickstream' else ['store_id', 'product_id', 'scanner_id']
    for field in required:
        if not isinstance(event.get(field), str) or not event[field].strip():
            return invalid('missing_' + field)
    allowed = CLICKSTREAM_TYPES if stream == 'clickstream' else INVENTORY_TYPES
    if event['event_type'] not in allowed:
        return invalid('invalid_event_type')
    if not re.fullmatch(r'\d+\.\d+\.\d+', event['schema_version']):
        return invalid('invalid_schema_version')
    try:
        timestamp = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
        if timestamp.tzinfo is None:
            return invalid('invalid_event_time')
        timestamp = timestamp.astimezone(timezone.utc)
    except (KeyError, AttributeError, ValueError, TypeError, OverflowError):
        return invalid('invalid_event_time')

    result = {field: event[field] for field in required}
    result.update(event_time=timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                  event_date=timestamp.date().isoformat(), error_reason=None)
    if stream == 'clickstream':
        if event['platform'] not in {'web', 'ios', 'android'}:
            return invalid('invalid_platform')
        if not re.fullmatch(r'\d+\.\d+\.\d+', event['app_version']):
            return invalid('invalid_app_version')
        properties = event.get('properties')
        if not isinstance(properties, dict):
            return invalid('invalid_properties')
        customer = event.get('customer_id')
        if customer is not None and not isinstance(customer, str):
            return invalid('invalid_customer_id')
        if event['event_type'] == 'checkout':
            order = properties.get('order_id')
            if not isinstance(order, str) or not re.fullmatch(r'ORDER-[A-Z0-9]{12}', order):
                return invalid('checkout_invalid_order_id')
            cart = properties.get('cart_value')
            if type(cart) not in (float, int) or not 0 <= cart <= 1e308 or not math.isfinite(cart):
                return invalid('checkout_invalid_cart_value')
        result.update(customer_id=customer.strip() or None if customer is not None else None,
                      client_id=event['client_id'].strip(),
                      properties=json.dumps(properties, separators=(',', ':'), sort_keys=True))
    else:
        delta = event.get('qty_delta')
        if type(delta) is not int or not -(2**31) <= delta < 2**31:
            return invalid('invalid_qty_delta')
        late = event.get('is_late', False)
        if not isinstance(late, bool):
            return invalid('invalid_is_late')
        result.update(qty_delta=delta, is_late=late)
    return result
