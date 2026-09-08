#!/usr/bin/env python3
"""Plot the paired backoff and memory evidence from audit_hardware_results.py."""
import argparse
import csv
import json
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def read(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def plot(out):
    audit = json.loads((out/'audit.json').read_text())
    pairs = [r for r in read(out/'pairs.csv') if r['queue']=='lubq' and r['scenario']=='1p32c'
             and all(r[k]=='True' for k in ('timing_quality_ok','stat-cpu_quality_ok','stat-cache_quality_ok'))]
    if len(pairs) < 2:
        raise ValueError('Need at least two usable LUBQ 1p32c pairs')
    memory = [r for r in read(out/'memory_summary.csv') if r['queue']=='lubq' and
              r['batch_size']=='0' and r['phase']=='full']
    mix = [r for r in read(out/'apx_metrics.csv') if
           r['job'].startswith('deep-ab-backoff-lubq-1p32c-instruction_mix') and
           r['bundle_complete']=='True' and not r['validation_issue'] and
           r['metric_id']=='apx.barrier_operations_percent']
    controls = {r['job']: float(r['value']) for r in mix if r['arm']=='crossbeam'}
    treatments = {r['job']: float(r['value']) for r in mix if r['arm']=='yield'}
    paired_mix = sorted(controls.keys() & treatments.keys())
    plt.rcParams.update({'font.family':'DejaVu Sans', 'font.size':11,
                         'axes.spines.top':False, 'axes.spines.right':False,
                         'svg.fonttype':'none', 'savefig.facecolor':'white'})
    fig, (a,b) = plt.subplots(1,2,figsize=(12.5,5.5),gridspec_kw={'width_ratios':[1,1.25]})
    fig.subplots_adjust(left=.15,right=.97,top=.79,bottom=.23,wspace=.43)
    fig.suptitle('Grace: two hardware costs exposed by a software experiment', x=.05, y=.98,
                 ha='left',fontsize=17,fontweight='bold')
    fig.text(.05,.905,f"LUBQ • {Path(audit['campaign']).name} • {audit['created_utc'][:10]}",color='#52606d')
    labels = ['Throughput','CPU time / handoff','User cycles / handoff','User instructions / handoff']
    fields = ['throughput_ratio','cpu_ns_ratio','cycles:u_ratio','instructions:u_ratio']
    colors = ['#296fa8','#169886','#8054b3']
    a.axvline(1,color='#8e99a4',ls='--',lw=1)
    for i, field in enumerate(fields):
        values = [float(r[field]) for r in pairs]
        a.plot([min(values),max(values)],[i,i],color='#b4bfc8',lw=2)
        for j,value in enumerate(values):
            a.scatter(value,i+(j-(len(values)-1)/2)*.10,s=42,color=colors[j%len(colors)],zorder=3)
        a.scatter(statistics.median(values),i,marker='|',s=220,color='#142838',zorder=4)
    a.set_yticks(range(4), labels)
    a.set_ylim(3.5,-.6); a.set_xlim(.5,1.8)
    a.set_xticks([.5,.75,1,1.25,1.5,1.75],['0.5','0.75','1.0','1.25','1.5','1.75'])
    a.set_xlabel('Yield policy / Crossbeam policy')
    a.set_title(f'Backoff change: 1 producer, 32 consumers\nBatch 256; {len(pairs)} paired allocations',loc='left',fontsize=12,pad=14)
    a.grid(axis='x',alpha=.16);a.set_axisbelow(True)
    mix_note = 'No complete paired Instruction Mix captures.'
    if paired_mix:
        bounds = lambda values: f'{min(values):.2f}–{max(values):.2f}%'
        mix_note = ('Barrier-operation share: '+bounds([controls[k] for k in paired_mix])+
                    ' → '+bounds([treatments[k] for k in paired_mix])+
                    f'\n({len(paired_mix)} separate paired Instruction Mix captures).')
    a.text(0,-.29,'Dots: allocation pairs. Black tick: median.\n'+mix_note,
           transform=a.transAxes,va='top',fontsize=9,color='#52606d')
    payloads = {4096:'32 KiB payload',65536:'512 KiB payload',524288:'4 MiB payload'}
    for depth,color,marker in zip(payloads,['#296fa8','#d18027','#169886'],['o','s','^']):
        rows = sorted((r for r in memory if int(r['depth'])==depth),key=lambda r:int(r['scenario'].split('p')[0]))
        x = [int(r['scenario'].split('p')[0]) for r in rows]
        y = [float(r['rss_delta_bytes_median'])/2**20 for r in rows]
        lo = [v-float(r['rss_delta_bytes_min'])/2**20 for r,v in zip(rows,y)]
        hi = [float(r['rss_delta_bytes_max'])/2**20-v for r,v in zip(rows,y)]
        b.errorbar(x,y,yerr=[lo,hi],color=color,marker=marker,label=payloads[depth],
                   markersize=7,capsize=3,lw=1.6,alpha=.85)
    b.plot([1,143],[.125,17.875],color='#566779',ls=':',lw=1.6,label='128 KiB × producers',zorder=0)
    b.set_xscale('log');b.set_yscale('log');b.set_xlim(.8,190);b.set_ylim(.08,29)
    b.set_xticks([1,16,143],['1','16','143']);b.set_yticks([.125,.5,2,8,18],['0.125','0.5','2','8','18'])
    b.minorticks_off();b.grid(alpha=.16);b.set_axisbelow(True)
    b.set_xlabel('Producers (one consumer)');b.set_ylabel('Full − constructed-empty RSS (MiB)')
    b.set_title('Memory footprint: fixed total payload\n64 KiB OS pages; three fresh processes per point',loc='left',fontsize=12,pad=14)
    b.legend(loc='upper left',fontsize=9,frameon=False)
    b.text(0,-.29,'RSS from smaps_rollup; process footprint, not queue-owned bytes.\nMedians with observed ranges; logarithmic axes.\nPayload = total queued u64 items × 8 bytes.',transform=b.transAxes,va='top',fontsize=9,color='#52606d')
    for ext in ('png','svg'):
        fig.savefig(out/('hardware_evidence.'+ext),dpi=180,bbox_inches='tight')
    plt.close(fig)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('analysis_dir',type=Path)
    plot(parser.parse_args().analysis_dir)
