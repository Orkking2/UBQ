#!/usr/bin/env python3
"""Offline audit of a copied hardware campaign; writes only a separate analysis directory."""
from __future__ import annotations
import argparse
import collections
import csv
import datetime as dt
import json
from pathlib import Path
import re
import statistics as stats
import hardware_campaign as h


def table(path, rows):
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def audit_logs(root, out):
    """Inventory log evidence by allocation; flags can overlap (e.g. OOM and -9)."""
    patterns = {'oom': r'oom_kill|Out.of.memory', 'time_limit': r'DUE TO TIME LIMIT',
                'cancelled': r'CANCELLED', 'apx_startup': r'apx version failed',
                'signal_9': r'exit status -9', 'unavailable': r'unavailable:'}
    files = sorted(p for p in root.rglob('*') if p.is_file())
    groups, evidence = collections.defaultdict(collections.Counter), []
    for path in files:
        group = re.sub(r'_\d+$', '', path.stem)
        data = path.read_text(errors='replace')
        groups[group]['files'] += 1
        flags = [key for key, pattern in patterns.items() if re.search(pattern, data, re.I)]
        groups[group].update(flags)
        if flags:
            evidence.append({'path': str(path.resolve()), 'categories': flags,
                             'lines': [line for line in data.splitlines()
                                       if any(re.search(p, line, re.I) for p in patterns.values())][:4]})
    summary = {'files': len(files), 'bytes': sum(p.stat().st_size for p in files),
               'groups': dict(sorted(groups.items())), 'evidence': evidence,
               'note': 'Counts are files with matching evidence, not a reconstruction of live processes.'}
    h.save(out/'log_audit.json', summary)


def audit(campaign, out, bench_root=None, logs=None):
    campaign, out = campaign.resolve(), out.resolve()
    if out == campaign or campaign in out.parents:
        raise ValueError('Use an analysis directory outside the copied campaign')
    out.mkdir(parents=True, exist_ok=True)
    plan, state, accounting = (h.read(campaign / name) for name in ('campaign.json', 'state.json', 'accounting.json'))
    h.verify_identity(campaign)
    h.reconcile(campaign, state, accounting)
    manifest = h.read(campaign / 'source-manifest.json')
    source_issues = [name for name, expected in manifest.items()
                     if not (campaign/'source'/name).is_file() or h.digest(campaign/'source'/name) != expected]
    binary_issues = [name for name, expected in h.read(campaign/'artifacts/binaries.json').items()
                     if h.digest(campaign/'artifacts'/('bench_hardware-'+name)) != expected]
    jobs, runs, memory, hardware, apx, errors, provenance_issues = [], [], [], [], [], [], []
    for case in plan['jobs']:
        id = case['id']
        directory = campaign/'results'/id
        entry = state['jobs'].get(id, {'status': 'unsubmitted'})
        sidecar = h.read(directory/'status.json') if (directory/'status.json').exists() else {}
        prov = h.read(directory/'provenance.json') if (directory/'provenance.json').exists() else {}
        if prov and (prov['source_manifest_sha256'] != plan['source_manifest_sha256'] or
                     prov['binaries'] != h.read(campaign/'artifacts/binaries.json')):
            provenance_issues.append(id)
        reason = sidecar.get('error', entry.get('reason', ''))
        jobs.append({'job': id, 'kind': case['kind'], 'status': entry['status'], 'sidecar': sidecar.get('status', ''),
                     'slurm': entry.get('slurm', ''), 'node': prov.get('node', ''), 'reason': reason})
        if reason and entry['status'] == 'failed':
            errors.append({'job': id, 'reason': reason})
        for path in sorted(directory.glob('*/benchmark.json')):
            result = h.read(path)
            run_status = h.read(path.parent/'status.json') if (path.parent/'status.json').exists() else {}
            cfg = h.read(path.parent/'workload.json')
            issue = ''
            try:
                h.validate_result(result, cfg, run_status.get('variant', 'base'))
            except RuntimeError as e:
                issue = str(e)
            match = re.fullmatch(r'r(\d+)-([^-]+)-(.+)', path.parent.name)
            base = {'job': id, 'kind': case['kind'], 'run': path.parent.name, 'path': str(path),
                    'node': prov.get('node', ''), 'queue': result['queue'], 'batch_size': result['batch_size'] or 0,
                    'scenario': result['scenario'], 'repeat': case.get('repeat', ''),
                    'arm': match[2] if match else '', 'collector': match[3] if match else 'diagnostic_or_pilot',
                    'bundle_complete': entry['status'] == 'completed', 'validation_issue': issue}
            if 'checkpoints' in result:
                checkpoints = {p['phase']: p for p in result['checkpoints']}
                empty = checkpoints['constructed_empty']['memory']['Rss_bytes']
                for checkpoint in result['checkpoints']:
                    phase = checkpoint['phase']
                    os_metrics = checkpoint.get('os') or {}
                    phase_index = {'full': 0, 'drained': 1, 'refilled': 2, 'redrained': 3}.get(phase)
                    memory.append({**base, 'depth': result['memory_depth'], 'phase': phase,
                                   'rss_bytes': checkpoint['memory']['Rss_bytes'],
                                   'private_bytes': checkpoint['memory']['Private_Clean_bytes'] + checkpoint['memory']['Private_Dirty_bytes'],
                                   'pss_bytes': checkpoint['memory']['Pss_bytes'],
                                   'rss_delta_bytes': checkpoint['memory']['Rss_bytes']-empty,
                                   'status_vmrss_bytes': checkpoint['memory'].get('VmRSS_bytes'),
                                   'phase_elapsed_ns': result['phase_elapsed_ns'][phase_index] if phase_index is not None else '', **os_metrics})
                continue
            os_metrics = result['os'] or {}
            n = result['items']
            row = {**base, 'items': n, 'rounds': result['rounds'], 'round_items': result['round_items'],
                   'duration_seconds': result['elapsed_ns']/1e9, 'handoffs_per_second': result['ops_per_sec'],
                   'cpu_ns_per_handoff': (os_metrics.get('user_cpu_ns',0)+os_metrics.get('system_cpu_ns',0))/n,
                   'system_cpu_fraction': os_metrics.get('system_cpu_ns',0)/max(1,os_metrics.get('user_cpu_ns',0)+os_metrics.get('system_cpu_ns',0)),
                   'voluntary_switches': os_metrics.get('voluntary_context_switches',0),
                   'barrier_proxy': result['barrier_overhead_proxy_fraction']}
            runs.append(row)
            hw_path = path.parent/'hardware.json'
            if hw_path.exists():
                raw = h.read(hw_path)
                for metric in raw.get('metrics', []):
                    apx.append({**base, **metric})
                denominator = n + result['warmup_rounds']*result['round_items']
                for event, value in raw.items():
                    if isinstance(value, dict) and 'value' in value:
                        row[event+'_per_handoff'] = value['value']/denominator
                        hardware.append({**base, 'event': event, **value, 'whole_process_handoffs': denominator,
                                         'per_handoff': value['value']/denominator})
    shadows = {(r['job'], r['arm']): r for r in runs if r['collector']=='shadow'}
    for row in runs:
        shadow = shadows.get((row['job'],row['arm']))
        ratio = row['duration_seconds']/shadow['duration_seconds'] if shadow else 0
        row['collector_shadow_duration_ratio'] = ratio
        row['quality_ok'] = bool(row['bundle_complete'] and not row['validation_issue'] and shadow and
                                 row['duration_seconds']>=5 and row['barrier_proxy']<=.05 and ratio<=3)
    pairs = []
    lookup = {(r['job'],r['arm'],r['collector']):r for r in runs}
    for case in plan['jobs']:
        if case['kind']!='ab': continue
        id = case['id']; control,treatment = [arm['name'] for arm in case['arms']]
        a,b = shadows.get((id,control)),shadows.get((id,treatment))
        if not a or not b: continue
        pair = {'job': id,'queue':a['queue'],'scenario':a['scenario'],'repeat':case['repeat'],'node':a['node'],
                'control':control,'treatment':treatment,'bundle_complete':a['bundle_complete'],
                'timing_quality_ok': a['quality_ok'] and b['quality_ok'] and a['items']==b['items'],
                'throughput_ratio':b['handoffs_per_second']/a['handoffs_per_second'],
                'cpu_ns_ratio':b['cpu_ns_per_handoff']/a['cpu_ns_per_handoff'],
                'control_Mhandoffs_s':a['handoffs_per_second']/1e6,'treatment_Mhandoffs_s':b['handoffs_per_second']/1e6,
                'control_cpu_ns':a['cpu_ns_per_handoff'],'treatment_cpu_ns':b['cpu_ns_per_handoff']}
        for collector, events in h.STAT.items():
            ac,bc = (lookup.get((id,arm,collector)) for arm in (control,treatment))
            if not ac or not bc: continue
            pair[collector+'_quality_ok'] = ac['quality_ok'] and bc['quality_ok']
            for event in events:
                key = event+'_per_handoff'
                if key in ac and key in bc:
                    pair[event+'_ratio'] = bc[key]/ac[key]
                    pair['control_'+event] = ac[key]
                    pair['treatment_'+event] = bc[key]
        pairs.append(pair)
    memory_summary = []
    groups = collections.defaultdict(list)
    for row in memory:
        if row['bundle_complete'] and not row['validation_issue']:
            groups[(row['queue'],row['batch_size'],row['scenario'],row['depth'],row['phase'])].append(row)
    for key, rows in sorted(groups.items()):
        summary = dict(zip(('queue','batch_size','scenario','depth','phase'),key))
        summary.update(n=len(rows))
        for field in ('rss_bytes','rss_delta_bytes','private_bytes','pss_bytes','minor_faults','phase_elapsed_ns'):
            values = [r[field] for r in rows if isinstance(r.get(field),(int,float))]
            if values:
                summary.update({field+'_median':stats.median(values),field+'_min':min(values),field+'_max':max(values)})
        memory_summary.append(summary)
    integrity = [row for row in errors if 'integrity mismatch' in row['reason'] or 'exact-once' in row['reason']]
    summary = {'campaign':str(campaign),'created_utc':dt.datetime.fromtimestamp(plan['created_epoch'],dt.timezone.utc).isoformat(),
               'saved_accounting_counts':dict(collections.Counter(v['state'] for k,v in accounting.items() if '.' not in k)),
               'panel_status':{kind:dict(collections.Counter(j['status'] for j in jobs if j['kind']==kind)) for kind in sorted(set(j['kind'] for j in jobs))},
               'source_files_checked':len(manifest),'source_issues':source_issues,'binary_issues':binary_issues,
               'provenance_issues':provenance_issues,'workload_validation_errors':sorted({r['path'] for r in runs+memory if r['validation_issue']}),
               'integrity_failure_count':len(integrity),'integrity_failures':integrity,
               'stale_running_sidecars':[j['job'] for j in jobs if j['sidecar']=='running' and j['status']=='failed'],
               'unique_nodes':len(set(j['node'] for j in jobs if j['node'])),
               'core_completed_shadows':sum(r['kind']=='core' and r['collector']=='shadow' and r['bundle_complete'] for r in runs),
               'core_shadow_quality_passes':sum(r['kind']=='core' and r['collector']=='shadow' and r['quality_ok'] for r in runs),
               'queue_integrity_caveat':'UBQ and Moodycamel batch paths have observed count failures; passing runs are not clearance of their correctness.',
               'memory_processes':len({(r['job'],r['run']) for r in memory}),
               'apx_startup_failures':sum(e['reason']=='apx version failed' for e in errors)}
    h.save(out/'audit.json',summary)
    for name,rows in (('jobs',jobs),('runs',runs),('memory',memory),('memory_summary',memory_summary),('counters',hardware),('apx_metrics',apx),('pairs',pairs)):
        table(out/(name+'.csv'),rows)
    if bench_root:
        inventory=[]
        for directory in sorted(bench_root.iterdir()):
            if not directory.is_dir():continue
            times=[];n=0;commits=set();dirty=set();records=list(directory.glob('*/record.json'))
            for path in records:
                record=h.read(path);n+=len(record['results']);commits.add(record['meta'].get('git_commit'));dirty.add(record['meta'].get('git_dirty'))
                times.extend(r['timestamp_unix_ms'] for r in record['results'] if 'timestamp_unix_ms' in r)
            utc=lambda t:dt.datetime.fromtimestamp(t/1000,dt.timezone.utc).isoformat()
            inventory.append({'dataset':directory.name,'scenario_files':len(records),'rows':n,
                              'first_utc':utc(min(times)) if times else '', 'last_utc':utc(max(times)) if times else '',
                              'commits':','.join(sorted(str(c) for c in commits)),'dirty':str(dirty)})
        table(out/'bench_inventory.csv',inventory)
    if logs:
        audit_logs(logs, out)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign',type=Path);parser.add_argument('out',type=Path)
    parser.add_argument('--bench-root',type=Path)
    parser.add_argument('--logs',type=Path)
    args=parser.parse_args()
    summary=audit(args.campaign,args.out,args.bench_root,args.logs)
    print(json.dumps(summary,indent=2))
