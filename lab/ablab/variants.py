"""Queue modules shared by scaffolding, Cargo, and study validation.

Each registry row is name, Rust module, source directory, adapter, FIFO class.
The registry lives in src/ so it is included in every frozen arm.
"""
from __future__ import annotations

import re
import shutil
import tempfile
from pathlib import Path

from .common import locked, write_json
from .spec import SpecError, WORKTREE

REGISTRY = 'src/queue_variants.tsv'
FIELDS = ('name', 'module', 'source', 'adapter', 'fifo')
# Prefix avoids Rust keywords, numeric names, and collisions with normal modules.
VARIANT_NAME = re.compile(r'^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$')
ORIGINAL_FILES = ('block.rs', 'head.rs', 'page.rs', 'slot.rs', 'queue.rs', 'tests.rs')


def entries(repo):
    result = []
    for line in (Path(repo) / REGISTRY).read_text().splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        fields = line.split('\t')
        if len(fields) != len(FIELDS):
            raise SpecError(f'invalid queue registry row: {line!r}')
        result.append(dict(zip(FIELDS, fields)))
    return result


def queue_name(entry):
    return 'ubq' if entry['name'] == 'ubq' else 'ubq-' + entry['name']


def lookup(rows, queue):
    # Longest match allows e.g. a variant named v8-something alongside v8.
    for entry in sorted(rows, key=lambda e: len(queue_name(e)), reverse=True):
        prefix = queue_name(entry)
        if queue == prefix or (entry['adapter'] == 'v8' and queue.startswith(prefix + '-s')):
            return entry
    raise KeyError(queue)


def _rewrite(text, source, target):
    """Relocate explicit self references, including grouped imports and doctests."""
    if source['module'] == 'crate':
        # Root UBQ depends on these private modules and the shared backoff module.
        # Bringing backoff into the fork keeps grouped crate imports local too.
        text = text.replace('crate::', f'crate::{target}::')
    else:
        text = re.sub(r'\bcrate::' + re.escape(source['module']) + r'\b',
                      f'crate::{target}', text)
        text = re.sub(r'\bubq::' + re.escape(source['module']) + r'\b',
                      f'ubq::{target}', text)
    return text


def create(repo, name, source_path, *, queue=None, force=False):
    """Serialize registry updates so simultaneous scaffolds cannot lose entries."""
    with locked(Path(repo) / 'results/.lab/new.lock', timeout=30):
        return _create(repo, name, source_path, queue=queue, force=force)


def _create(repo, name, source_path, *, queue=None, force=False):
    """Copy a registered implementation and create a paired WORKTREE study.

    Validate before writing. --force only replaces a spec, never source code.
    Staging and rollback leave existing modules/specs intact on copy failures.
    """
    repo = Path(repo).resolve()
    if len(name) > 48 or not VARIANT_NAME.fullmatch(name):
        raise SpecError('variant name must be 1-48 lowercase letters, digits, hyphens or underscores, starting with a letter')
    rows = entries(repo)
    source_path = Path(source_path).resolve()
    source = next((e for e in rows if (repo / e['source']).resolve() == source_path), None)
    if source is None:
        raise SpecError('--src must name a registered queue directory; see ./ab queues')
    module = 'ab_' + name.replace('-', '_')
    if any(e['name'] == name or e['module'] == module for e in rows):
        raise SpecError(f'queue variant {name!r} is already registered')
    if any(e['adapter'] == 'v8' and re.fullmatch(
            re.escape(queue_name(e)) + r'-s[0-9]+(?:-v[0-9]+)?', 'ubq-' + name) for e in rows):
        raise SpecError(f'queue variant {name!r} conflicts with an existing geometry selector')
    target = repo / 'src' / name
    if target.exists() or target.is_symlink():
        raise SpecError(f'{target} exists; source modules are never overwritten')
    study = repo / 'lab/studies' / f'{name}.json'
    if study.exists() and not force:
        raise SpecError(f'{study} exists; pass --force to replace only the study spec')
    base_queue = queue or queue_name(source) + ('-s4096' if source['adapter'] == 'v8' else '')
    try:
        selected = lookup(rows, base_queue)
    except KeyError:
        selected = None
    if selected != source:
        raise SpecError('--queue must select the --src implementation')
    if source['adapter'] == 'v8' and base_queue != queue_name(source):
        if not re.fullmatch(re.escape(queue_name(source)) + r'-s[1-9][0-9]*(?:-v[1-9][0-9]*)?', base_queue):
            raise SpecError('invalid queue geometry; use -s<SLOTS>[-v<BLOCKS>] with positive integers')
    entry = dict(name=name, module=module, source=f'src/{name}', adapter=source['adapter'], fifo=source['fifo'])
    suffix = base_queue[len(queue_name(source)):]
    tests = ['tests::', 'block::', 'head::'] if source['module'] == 'crate' else [source['module'] + '::']
    raw = {
        'name': name,
        'question': 'What does the candidate change, and where should it help?',
        'arms': [
            {'name': 'base', 'ref': WORKTREE, 'workload': {'queue': base_queue}},
            {'name': 'cand', 'ref': WORKTREE, 'workload': {'queue': queue_name(entry) + suffix}},
        ],
        'workload': {'queue': base_queue},
        'build': {'tests': tests + [module + '::', 'bench_harness::hardware']},
    }
    registry_path = repo / REGISTRY
    previous_registry = registry_path.read_text()
    previous_study = study.read_bytes() if study.exists() else None
    with tempfile.TemporaryDirectory(prefix='.ab-new-', dir=repo / 'src') as tmp:
        staging = Path(tmp) / name
        if source['module'] == 'crate':
            staging.mkdir()
            for filename in ORIGINAL_FILES:
                shutil.copy2(source_path / filename, staging / filename)
            (staging / 'mod.rs').write_text(
                '//! Independent fork of the original UBQ.\n'
                'pub(crate) use crate::backoff;\n'
                'mod block;\nmod head;\nmod page;\nmod slot;\nmod queue;\n'
                '#[cfg(test)]\nmod tests;\npub use queue::UBQ;\n')
        else:
            shutil.copytree(source_path, staging)
        for path in staging.rglob('*'):
            if path.suffix == '.rs' and (path.name != 'mod.rs' or source['module'] != 'crate'):
                path.write_text(_rewrite(path.read_text(), source, module))
        installed = False
        try:
            staging.rename(target)
            installed = True
            registry_path.write_text(previous_registry.rstrip() + '\n' + '\t'.join(entry[k] for k in FIELDS) + '\n')
            write_json(study, raw)
        except BaseException:
            if installed:
                shutil.rmtree(target, ignore_errors=True)
            registry_path.write_text(previous_registry)
            if previous_study is None:
                study.unlink(missing_ok=True)
            else:
                study.write_bytes(previous_study)
            raise
    return target, study
