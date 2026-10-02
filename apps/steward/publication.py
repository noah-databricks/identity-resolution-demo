"""Pure, allowlisted conversion from committed outbox events to Gold overrides.

No source identity fields, agent messages or evaluation metadata are exported.
The event sink must persist the event id and digest before acknowledging Lakebase.
"""
import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def export_event(event: dict) -> dict:
    proposal = event['proposal']
    # Golden-field selections are now materialized in reviewed_golden_fields table
    # and will be queried during Gold publication.
    action = proposal['action']
    if action not in ('MERGE', 'SEPARATE', 'PARTITION', 'DEFER'):
        raise ValueError('Unknown committed action')
    if proposal['case_id'] != event['case_id']:
        raise ValueError('Proposal/case mismatch')
    if (event['applied_case_version'] != event['base_case_version'] + 1
            or proposal['case_version'] != event['base_case_version']):
        raise ValueError('Invalid committed version sequence')
    groups = proposal['groups']
    if not groups or any(not group for group in groups):
        raise ValueError('Empty partition')
    keys = [key for group in groups for key in group]
    if any(not isinstance(key, str) or not key for key in keys) or len(set(keys)) != len(keys):
        raise ValueError('Invalid or duplicate record membership')
    if action == 'MERGE' and len(groups) != 1:
        raise ValueError('Merge requires one group')
    if action == 'SEPARATE' and any(len(group) != 1 for group in groups):
        raise ValueError('Separate requires singleton groups')
    if action == 'PARTITION' and (len(groups) < 2 or not any(len(g) > 1 for g in groups)):
        raise ValueError('Partition requires multiple groups, including a non-singleton')
    memberships = []
    if action != 'DEFER':
        for group in groups:
            # Stable under group order, record order and retries. Scoped to release.
            identity = 'steward-' + digest([event['source_run_id'], sorted(group)])
            memberships.extend({'record_key': key, 'customer_id': identity} for key in group)
    row = {key: event[key] for key in ('event_id', 'case_id', 'source_run_id',
                                     'base_case_version', 'applied_case_version', 'actor', 'mode')}
    row.update(action=action, proposal_id=proposal['proposal_id'],
               trace_id=proposal.get('trace_id'),
               memberships=sorted(memberships, key=lambda m: m['record_key']))
    row['content_digest'] = digest(row)
    return row


def latest_overrides(events):
    """Reference semantics for the SQL sink, including out-of-order retries."""
    seen, versions, latest = {}, {}, {}
    for raw in events:
        event = export_event(raw)
        event_id = event['event_id']
        if event_id in seen:
            if seen[event_id] != event['content_digest']:
                raise ValueError('Event id reused with different content')
            continue
        seen[event_id] = event['content_digest']
        version_key = (event['source_run_id'], event['case_id'], event['applied_case_version'])
        if version_key in versions:
            raise ValueError('Multiple events for the same case version')
        versions[version_key] = event_id
        if event['action'] == 'DEFER':
            continue
        key = (event['source_run_id'], event['case_id'])
        if key not in latest or event['applied_case_version'] > latest[key]['applied_case_version']:
            latest[key] = event
    result = {}
    for event in latest.values():
        for member in event['memberships']:
            key = (event['source_run_id'], member['record_key'])
            if key in result:
                raise ValueError('Overlapping cases require reconciliation before publication')
            result[key] = {**member, 'event_id': event['event_id'],
                           'case_id': event['case_id'],
                           'applied_case_version': event['applied_case_version']}
    return result
