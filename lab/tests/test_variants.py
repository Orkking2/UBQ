"""Scaffolding regressions, plus opt-in compilation of real independent forks."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ablab import cli, snapshot, spec, variants
from ablab.common import read_json

REPO = Path(__file__).resolve().parents[2]


class VariantTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name)
        (self.repo / 'src/v8/tests').mkdir(parents=True)
        # Only the original queue and v8: the real registry may already list forks.
        rows = [line for line in (REPO / variants.REGISTRY).read_text().splitlines()
                if line.startswith('#') or line.split('\t')[0] in ('ubq', 'v8')]
        (self.repo / variants.REGISTRY).write_text('\n'.join(rows) + '\n')
        for relative in ('mod.rs', 'tests/freelist.rs'):
            source = self.repo / 'src/v8' / relative
            source.write_text('use crate::v8::Config;\n//! use ubq::v8::UBQ;\n')
        for filename in variants.ORIGINAL_FILES:
            (self.repo / 'src' / filename).write_text('use crate::{block::Block, backoff};\n')

    def create(self, name='example-fork', source='src/v8', **kwargs):
        return variants.create(self.repo, name, self.repo / source, **kwargs)

    def test_copy_relocates_tests_and_builds_a_same_harness_pair(self):
        before = (self.repo / 'src/v8/mod.rs').read_bytes()
        target, study = self.create(queue='ubq-v8-s64-v3')
        raw = read_json(study)
        spec.validate(raw, lambda q: variants.lookup(variants.entries(self.repo), q)['fifo'])
        self.assertEqual([a['ref'] for a in raw['arms']], ['WORKTREE', 'WORKTREE'])
        self.assertEqual([a['workload']['queue'] for a in raw['arms']],
                         ['ubq-v8-s64-v3', 'ubq-example-fork-s64-v3'])
        self.assertIn('ab_example_fork::', raw['build']['tests'])
        self.assertIn('crate::ab_example_fork::Config', (target / 'tests/freelist.rs').read_text())
        self.assertIn('ubq::ab_example_fork::UBQ', (target / 'mod.rs').read_text())
        (target / 'mod.rs').write_text('candidate edit')
        self.assertEqual((self.repo / 'src/v8/mod.rs').read_bytes(), before)

    def test_fork_of_fork_inherits_adapter_geometry_and_fifo(self):
        first, _ = self.create()
        registry = self.repo / variants.REGISTRY
        registry.write_text(registry.read_text().replace('src/example-fork\tv8\tstrict', 'src/example-fork\tv8\trelaxed'))
        target, study = self.create('next', 'src/example-fork')
        self.assertIn('crate::ab_next::Config', (target / 'tests/freelist.rs').read_text())
        self.assertEqual(variants.entries(self.repo)[-1]['fifo'], 'relaxed')
        self.assertEqual(read_json(study)['arms'][0]['workload']['queue'], 'ubq-example-fork-s4096')

    def test_original_copies_only_queue_files_and_relocates_private_imports(self):
        target, study = self.create('original-fork', 'src')
        self.assertEqual({p.name for p in target.iterdir()}, set(variants.ORIGINAL_FILES) | {'mod.rs'})
        self.assertIn('crate::ab_original_fork::{block::Block, backoff}', (target / 'queue.rs').read_text())
        self.assertIn('use crate::backoff', (target / 'mod.rs').read_text())
        self.assertEqual(read_json(study)['arms'][1]['workload']['queue'], 'ubq-original-fork')

    def test_invalid_inputs_leave_no_files(self):
        previous = (self.repo / variants.REGISTRY).read_bytes()
        for name in ('../escape', 'UPPER', 'bad.name', 'a' * 49, 'v8-s64', 'v8-s64-v3'):
            with self.assertRaises(spec.SpecError):
                self.create(name)
        for queue in ('ubq', 'unknown', 'ubq-v8-s0', 'ubq-v8-s64-vx'):
            with self.assertRaises(spec.SpecError):
                self.create(queue=queue)
        with self.assertRaises(spec.SpecError):
            self.create(source='src/unknown')
        self.assertEqual((self.repo / variants.REGISTRY).read_bytes(), previous)
        self.assertFalse((self.repo / 'src/example-fork').exists())

    def test_force_never_overwrites_source_or_module_alias_collision(self):
        target, study = self.create()
        before = (target / 'mod.rs').read_bytes()
        for name in ('example-fork', 'example_fork'):
            with self.assertRaises(spec.SpecError):
                self.create(name, force=True)
        self.assertEqual((target / 'mod.rs').read_bytes(), before)

    def test_spec_collision_and_rollback(self):
        study = self.repo / 'lab/studies/example-fork.json'
        study.parent.mkdir(parents=True)
        study.write_text('existing study')
        with self.assertRaises(spec.SpecError):
            self.create()
        registry = (self.repo / variants.REGISTRY).read_bytes()
        with mock.patch.object(variants, 'write_json', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.create(force=True)
        self.assertEqual(study.read_text(), 'existing study')
        self.assertEqual((self.repo / variants.REGISTRY).read_bytes(), registry)
        self.assertFalse((self.repo / 'src/example-fork').exists())
        self.create(force=True)
        self.assertEqual(read_json(study)['name'], 'example-fork')

    def test_revision_studies_remain_supported(self):
        with mock.patch.object(cli, 'REPO', self.repo), mock.patch.object(cli, 'LAB', self.repo / 'lab'), \
                mock.patch.object(cli, 'fifo_class', return_value=None):
            self.assertEqual(cli.main(['new', 'revision']), 0)
        self.assertEqual(read_json(self.repo / 'lab/studies/revision.json')['arms'][0]['ref'], 'HEAD')


@unittest.skipUnless(os.environ.get('AB_E2E'), 'AB_E2E=1 compiles real queue forks')
class VariantCompileTests(unittest.TestCase):
    def test_real_forks_build_and_verify_scalar_batch_and_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            for directory in ('src', 'benches', 'third_party', '.cargo'):
                shutil.copytree(REPO / directory, repo / directory)
            for filename in ('Cargo.toml', 'Cargo.lock', 'build.rs'):
                shutil.copy2(REPO / filename, repo / filename)
            (repo / 'vendor').symlink_to(REPO / 'vendor', target_is_directory=True)
            variants.create(repo, 'owned-fork', repo / 'src/v8')
            variants.create(repo, 'original-fork', repo / 'src')
            variants.create(repo, 'second-fork', repo / 'src/owned-fork')
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Lab Test',
                            '-c', 'user.email=lab@example.invalid', 'commit', '--allow-empty',
                            '-qm', 'Snapshot fixture'], check=True)
            vendor = snapshot.vendor_hash(repo)
            frozen_root = repo / 'frozen'
            (frozen_root / 'vendor').mkdir(parents=True)
            (frozen_root / 'vendor' / vendor).symlink_to(REPO / 'vendor', target_is_directory=True)
            frozen = snapshot.snapshot(repo, 'WORKTREE', frozen_root / 'sources', vendor)
            # Editing a candidate changes source identity, but never the harness.
            with (repo / 'src/owned-fork/queue.rs').open('a') as handle:
                handle.write('\n// independent candidate edit\n')
            changed = snapshot.snapshot(repo, 'WORKTREE', frozen_root / 'sources', vendor)
            self.assertNotEqual(frozen['source'], changed['source'])
            self.assertEqual(frozen['harness'], changed['harness'])
            repo = frozen_root / 'sources' / changed['source']
            self.assertIn('owned-fork', (repo / variants.REGISTRY).read_text())
            # Compile copied unit tests as well as the actual CLI target.
            env = dict(os.environ, CARGO_TARGET_DIR=str(REPO / 'target/ab-variant-tests'))
            for args in (['test', '--lib', '--no-run'], ['build', '--bin', 'bench_hardware'],
                         ['check', '--no-default-features', '--lib']):
                subprocess.run(['cargo', *args, '--offline', '--locked'], cwd=repo, env=env, check=True, timeout=240)
            binary = Path(env['CARGO_TARGET_DIR']) / 'debug/bench_hardware'
            for queue in ('ubq', 'ubq-v8-s64-v3', 'ubq-owned-fork-s64-v3', 'ubq-original-fork', 'ubq-second-fork-s64-v3'):
                for batch in (None, 16):
                    for mode in ('verify', 'memory'):
                        config = {'queue': queue, 'mode': mode, 'scenario': '2p2c',
                                  'producer_batch': batch, 'consumer_batch': batch,
                                  'round_items': 1024, 'rounds': 2, 'warmup_rounds': 1,
                                  'memory_depth': 1024, 'idle_secs': [], 'rss_limit_mib': 0, 'allow_unpinned': True}
                        path = repo / 'config.json'
                        path.write_text(json.dumps(config))
                        result = subprocess.run([str(binary), '--config', str(path)], capture_output=True,
                                                text=True, check=True, timeout=30)
                        self.assertEqual(json.loads(result.stdout)['queue'], queue)
                        if mode == 'verify':
                            self.assertTrue(json.loads(result.stdout)['exact_once_checked'])


if __name__ == '__main__':
    unittest.main()
