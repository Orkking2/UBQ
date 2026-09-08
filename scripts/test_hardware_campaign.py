"""Protocol and Slurm regressions, independent of a live cluster."""
import contextlib
import argparse
import io
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import hardware_campaign as h


class CampaignTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('cargo'), 'Cargo is required to validate the frozen manifest')
    def test_prepared_snapshot_resolves_all_declared_cargo_targets(self):
        # Exercise the real packaging path, rather than building from the checkout:
        # Cargo validates benches even for --lib tests and --bin bench_hardware.
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(campaign=Path(tmp)/'campaign', cluster='grace',
                                      hours=48, account='test', partition=None,
                                      qos=None, constraint='perfparanoid')
            campaign = h.prepare(args)
            source = campaign/'source'
            result = subprocess.run(['cargo', 'metadata', '--offline', '--locked',
                                     '--no-deps', '--format-version=1'], cwd=source,
                                    text=True, capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            package = next(p for p in json.loads(result.stdout)['packages'] if p['name']=='ubq')
            manifest = h.read(campaign/'source-manifest.json')
            self.assertTrue(any('bench' in target['kind'] for target in package['targets']))
            for target in package['targets']:
                path = Path(target['src_path'])
                self.assertTrue(path.is_file(), path)
                relative = path.relative_to(source)
                self.assertEqual(manifest[str(relative)], h.digest(path))
                self.assertFalse(path.samefile(h.ROOT/relative))
            self.assertEqual((source/'README.md').read_bytes(), (h.ROOT/'README.md').read_bytes())
            h.verify_identity(campaign)

    @unittest.skipUnless(os.environ.get('UBQ_HARDWARE_TEST_BINARY'), 'set UBQ_HARDWARE_TEST_BINARY for executable integration')
    def test_real_process_replay_memory_and_paired_csv_export(self):
        import shutil
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'source').mkdir();(root/'artifacts').mkdir()
            shutil.copy2(os.environ['UBQ_HARDWARE_TEST_BINARY'],root/'artifacts/bench_hardware-base')
            jobs=[];state={'jobs':{}}
            for rep in range(3):
                id=f'ab-test-r{rep}';directory=root/'results'/id
                job={'id':id,'kind':'ab','arms':[{'name':'crossbeam'},{'name':'yield'}]}
                jobs.append(job);state['jobs'][id]={'status':'completed'}
                h.save(directory/'provenance.json',{'node':'local-test'})
                for arm in job['arms']:
                    config=h.workload({'queue':'lubq','batch_size':256},'2p2c',backoff=arm['name'],
                                      round_items=16384,rounds=4,allow_unpinned=True,rss_limit_mib=0)
                    result=h.measure(root,directory/f"r{rep}-{arm['name']}-shadow",config)
                    self.assertEqual(result['items'],65536)
            id='memory-test';directory=root/'results'/id
            jobs.append({'id':id,'kind':'memory'});state['jobs'][id]={'status':'completed'}
            h.save(directory/'provenance.json',{'node':'local-test'})
            config=h.workload({'queue':'memory-scaffold'},'2p2c',mode='memory',memory_depth=4133,
                              idle_secs=[],allow_unpinned=True,rss_limit_mib=0)
            h.measure(root,directory/'r0-depth4133',config)
            h.save(root/'campaign.json',{'jobs':jobs});h.save(root/'state.json',state)
            with contextlib.redirect_stdout(io.StringIO()):h.report(root)
            import csv
            effects=list(csv.DictReader((root/'effects.csv').read_text().splitlines()))
            self.assertEqual(len(effects),3)
            self.assertTrue(all(float(r['throughput_treatment_over_control'])>0 for r in effects))
            self.assertIn('destroyed',(root/'memory.csv').read_text())

    def test_accounting_expands_array_ids_and_includes_oom_steps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            response=subprocess.CompletedProcess([],0,'123_0|COMPLETED|0:0|node1\n123_0.batch|OUT_OF_MEMORY|0:9|node1\n','')
            with patch.object(h,'run',return_value=response) as call:
                result=h.accounting(root,{'jobs':{'a':{'slurm':'123_0'}},'build':None})
            cmd=call.call_args[0][0]
            self.assertIn('--array',cmd)
            self.assertNotIn('-X',cmd)
            self.assertIn('--format=JobID%80,State%32,ExitCode,NodeList',cmd)
            self.assertEqual(result['123_0']['state'],'OUT_OF_MEMORY')
            self.assertEqual(result['123_0']['allocation_state'],'COMPLETED')

    def test_plan_has_real_endpoints_and_closed_dependencies(self):
        for cluster, cores in (("grace",144),("mn5",112)):
            p=h.make_plan(cluster)
            jobs={j['id']:j for j in p['jobs']}
            self.assertEqual(len(jobs),len(p['jobs']))
            self.assertEqual(sum(j['kind']=='core' for j in jobs.values()),504)
            self.assertEqual(sum(j['kind']=='memory' for j in jobs.values()),99)
            for j in jobs.values():
                # `requires` entries are either a job id or an any-of group of them.
                self.assertTrue(set(h.required_ids(j)) <= jobs.keys())
            for sc in (f'{cores-1}p1c',f'1p{cores-1}c',f'{cores//2}p{cores//2}c'):
                self.assertEqual(sum(j.get('scenario')==sc and j['kind']=='core' for j in jobs.values()),42)
            for j in jobs.values():
                if j['kind'] in ('core','ab') and j['repeat']:
                    self.assertIn(j['schedule_parent'],j['requires'])

    def test_terminal_slurm_overrides_stale_running_and_false_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            h.save(root/'results/a/status.json',{'status':'running'})
            h.save(root/'results/b/status.json',{'status':'completed'})
            state={'jobs':{'a':{'slurm':'123_0'},'b':{'slurm':'123_1'},'c':{'slurm':'123_2'}}}
            h.reconcile(root,state,{'123_0':{'state':'TIMEOUT','exit_code':'0:9'},'123_1':{'state':'OUT_OF_MEMORY','exit_code':'0:0'}})
            self.assertEqual(state['jobs']['a']['status'],'failed')
            self.assertEqual(state['jobs']['b']['status'],'failed')
            self.assertEqual(state['jobs']['c']['status'],'unknown')

    def test_failed_correctness_blocks_but_optional_counter_does_not(self):
        state={'jobs':{'gate':{'status':'completed'},'cap-stat-cpu':{'status':'failed'}}}
        core={'kind':'core','requires':['gate','cap-stat-cpu']}
        self.assertEqual(h.eligible(core,state),'ready')
        state['jobs']['gate']['status']='failed'
        self.assertEqual(h.eligible(core,state),'skip')
        self.assertEqual(h.eligible({'kind':'deep','requires':['cap-stat-cpu']},state),'skip')

    def test_quota_halves_admission_without_running_throttle_or_dependencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);state={'jobs':{},'build':None}
            jobs=[{'id':f'job{i}','seconds':3300} for i in range(9)]
            p=dict(account='acct',partition='gpp',qos='test',worker_cores=112,constraint='perfparanoid')
            calls=[]
            def fake_run(command,**kwargs):
                calls.append(command)
                n=int(next(c for c in command if c.startswith('--array=')).split('-')[-1])+1
                return subprocess.CompletedProcess(command,0 if n<=2 else 1,'123\n' if n<=2 else '', '' if n<=2 else 'QOSMaxSubmitJobPerUserLimit')
            with patch.object(h,'run',fake_run):
                self.assertEqual(h.submit(root,p,state,jobs),2)
            self.assertEqual([next(c for c in cmd if c.startswith('--array=')) for cmd in calls],['--array=0-8','--array=0-3','--array=0-1'])
            for command in calls:
                self.assertFalse(any(c.startswith('--dependency') for c in command))
                self.assertFalse('%' in next(c for c in command if c.startswith('--array=')))
                self.assertIn('--exclusive',command)
                self.assertIn('--cpus-per-task=112',command)
            self.assertEqual(state['jobs']['job1']['slurm'],'123_1')
            self.assertFalse((root/'submit-intent.json').exists())

    def test_unknown_submission_acceptance_is_not_resubmitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            h.save(root/'submit-intent.json',{'ids':['a'],'token':'unique','time':0})
            with patch.object(h,'run',return_value=subprocess.CompletedProcess([],0,'','')):
                with self.assertRaisesRegex(RuntimeError,'unresolved submission'):
                    h.recover_intent(root,{'jobs':{}})

    def test_low_counter_running_fraction_and_missing_events_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'perf.csv'
            path.write_text('100;;cycles:u;100000;99.5;;\n200;;instructions:u;100000;99.5;;\n')
            self.assertEqual(h.validate_perf_csv(path,h.STAT['stat-cpu'])['cycles:u']['value'],100)
            path.write_text('100;;cycles:u;100000;50.0;;\n')
            with self.assertRaises(RuntimeError):h.validate_perf_csv(path,h.STAT['stat-cpu'])

    def test_perf_event_names_missing_their_modifier_suffix_are_still_matched(self):
        # perf's `-x` CSV output has been observed to print some events (for
        # example dTLB-loads:u) without the requested `:u` modifier suffix.
        # A real, fully time-running counter must not be discarded just
        # because the reported name does not match the requested one exactly.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'perf.csv'
            path.write_text('7339894217;;dTLB-loads;100000;100.0;;\n9830920;;dTLB-load-misses;100000;100.0;;\n')
            result = h.validate_perf_csv(path, h.STAT['stat-tlb'])
            self.assertEqual(result['dTLB-loads:u']['value'], 7339894217)
            self.assertEqual(result['dTLB-load-misses:u']['value'], 9830920)

    def test_mem_flag_is_requested_on_grace_and_omitted_on_mn5(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for cluster, expect_mem in (('grace', True), ('mn5', False)):
                plan = dict(account='acct', partition='gpp', qos='test', worker_cores=112,
                            constraint='perfparanoid', cluster=cluster)
                calls = []
                def fake_run(command, **kwargs):
                    calls.append(command)
                    return subprocess.CompletedProcess(command, 0, '123\n', '')
                with patch.object(h, 'run', fake_run):
                    h.sbatch(root, plan, 'job', Path('script.sh'))
                self.assertEqual('--mem=0' in calls[0], expect_mem)
            # A plan without a recorded cluster keeps the original behavior.
            plan = dict(account='acct', partition='gpp', qos='test', worker_cores=112, constraint='perfparanoid')
            with patch.object(h, 'run', fake_run):
                h.sbatch(root, plan, 'job', Path('script.sh'))
            self.assertIn('--mem=0', calls[-1])

    def test_system_utilization_is_wired_into_the_grace_deep_panel(self):
        self.assertIn('system_utilization', h.APX)
        plan = h.make_plan('grace')
        jobs = {j['id']: j for j in plan['jobs']}
        self.assertIn('cap-system_utilization', jobs)
        deep_system_utilization = [j for j in jobs.values()
                                    if j['kind'] == 'deep' and j.get('collector') == 'system_utilization']
        self.assertTrue(deep_system_utilization)
        for job in deep_system_utilization:
            # Its pilot is an any-of group over independent allocations.
            self.assertIn('cap-system_utilization', h.required_ids(job))
            self.assertIn(h.cap_ids('system_utilization'), job['requires'])
        # MN5 stays on the perf-record family; APX system_utilization is Grace-only.
        mn5_jobs = {j['id']: j for j in h.make_plan('mn5')['jobs']}
        self.assertNotIn('cap-system_utilization', mn5_jobs)

    def test_streaming_timeout_preserves_output_and_terminates_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with self.assertRaisesRegex(RuntimeError,'timed out'):
                h.run([sys.executable,'-u','-c','import time; print("checkpoint",flush=True); time.sleep(30)'],
                      timeout=.2,stdout_path=root/'stdout',stderr_path=root/'stderr')
            self.assertIn('checkpoint',(root/'stdout').read_text())
            self.assertTrue(h.read(root/'stdout.meta.json')['timed_out'])
            self.assertIsNone(h.ACTIVE)

    def test_fixed_work_and_exact_once_cannot_be_faked_by_success_exit(self):
        config=h.workload({'queue':'ubq','batch_size':256},'2p2c',round_items=4096,rounds=4,mode='verify',allow_unpinned=True)
        result=dict(config=config,round_items=4096,rounds=4,items=16384,elapsed_ns=100,fixed_work=True,
                    exact_once_checked=True,completion_accounting='segment')
        h.validate_result(result,config)
        for key,value in (('rounds',3),('items',1),('exact_once_checked',False),('completion_accounting','item')):
            with self.assertRaises(RuntimeError):h.validate_result({**result,key:value},config)


if __name__=='__main__':unittest.main()
