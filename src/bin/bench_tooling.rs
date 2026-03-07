#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

pub const DEFAULT_SCENARIOS: &[&str] = &[
    "1p1c", "4p1c", "1p4c", "4p4c", "8p1c", "8p4c", "8p8c", "1p8c", "4p8c", "16p1c", "1p16c",
    "8p16c", "16p8c", "16p16c", "32p1c", "1p32c", "16p32c", "32p16c", "32p32c", "64p1c", "1p64c",
    "32p64c", "64p32c", "64p64c",
];

const UBQ_VERSIONS: [u8; 5] = [3, 4, 5, 6, 7];
const UBQ_POOL_VALUES: [u8; 7] = [1, 2, 4, 8, 16, 32, 64];
const UBQ_BLOCK_VALUES: [u16; 8] = [31, 63, 127, 255, 511, 1023, 2047, 4095];
const UBQ_BACKOFF_VALUES: [&str; 2] = ["", "b"];

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UbqLabel {
    pub version: u8,
    pub pool: u8,
    pub block: u16,
    pub backoff: String,
}

impl UbqLabel {
    pub fn text(&self) -> String {
        if self.backoff.is_empty() {
            format!("v{},{},{}", self.version, self.pool, self.block)
        } else {
            format!(
                "v{},{},{},{}",
                self.version, self.pool, self.block, self.backoff
            )
        }
    }

    pub fn safe(&self) -> String {
        if self.backoff.is_empty() {
            format!("v{}_{}_{}", self.version, self.pool, self.block)
        } else {
            format!(
                "v{}_{}_{}_{}",
                self.version, self.pool, self.block, self.backoff
            )
        }
    }

    pub fn feature_csv(&self) -> String {
        let mut features = vec![format!("ubq_v{}", self.version)];
        if self.version != 6 {
            features.push(format!("ubq_pool_{}", self.pool));
        }
        features.push(format!("ubq_block_{}", self.block));
        if self.backoff == "b" {
            features.push("ubq_backoff_cq".to_string());
        }
        features.join(",")
    }
}

pub fn parse_ubq_label(token: &str, require_valid: bool) -> Result<UbqLabel, String> {
    let text = token.trim().to_ascii_lowercase();
    let parts: Vec<&str> = text
        .split([',', '_'])
        .filter(|s| !s.trim().is_empty())
        .collect();

    if parts.len() < 3 || parts.len() > 4 {
        return Err(format!("invalid UBQ label '{token}'"));
    }

    let version = parts[0]
        .strip_prefix('v')
        .ok_or_else(|| format!("invalid UBQ label '{token}'"))?
        .parse::<u8>()
        .map_err(|_| format!("invalid UBQ label '{token}'"))?;
    let pool = parts[1]
        .parse::<u8>()
        .map_err(|_| format!("invalid UBQ label '{token}'"))?;
    let block = parts[2]
        .parse::<u16>()
        .map_err(|_| format!("invalid UBQ label '{token}'"))?;
    let backoff = parts.get(3).copied().unwrap_or("").to_string();

    let label = UbqLabel {
        version,
        pool,
        block,
        backoff,
    };

    if require_valid && !is_valid_ubq_label(&label) {
        return Err(format!("invalid UBQ label '{token}'"));
    }

    Ok(label)
}

pub fn normalize_ubq_label(token: &str, require_valid: bool) -> Option<String> {
    parse_ubq_label(token, require_valid).ok().map(|v| v.text())
}

pub fn is_valid_ubq_label(label: &UbqLabel) -> bool {
    if !UBQ_VERSIONS.contains(&label.version) {
        return false;
    }
    if !UBQ_BLOCK_VALUES.contains(&label.block) {
        return false;
    }
    if !UBQ_BACKOFF_VALUES.contains(&label.backoff.as_str()) {
        return false;
    }
    if label.version == 6 {
        return label.pool == 0;
    }
    UBQ_POOL_VALUES.contains(&label.pool)
}

pub fn bench_label_sort_key(label: &str) -> (u8, u8, u16, u8, String) {
    match parse_ubq_label(label, false) {
        Ok(parsed) => {
            let backoff_idx = if parsed.backoff == "b" { 1 } else { 0 };
            (
                parsed.version,
                parsed.pool,
                parsed.block,
                backoff_idx,
                parsed.backoff,
            )
        }
        Err(_) => (255, 255, u16::MAX, 255, label.to_string()),
    }
}

#[derive(Clone, Debug)]
pub struct Stats {
    pub mean_ops_per_sec: f64,
}

pub type GroupedRuns =
    BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Stats>>>>;

pub fn normalize_machine(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn normalize_scenario(name: &str) -> String {
    let key = name.trim().to_ascii_lowercase();
    match key.as_str() {
        "spsc" => "1p1c".to_string(),
        "mpsc" => "4p1c".to_string(),
        "spmc" => "1p4c".to_string(),
        "mpmc" => "4p4c".to_string(),
        _ => key,
    }
}

pub fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

pub fn parse_scenarios(raw: Option<&str>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let source: Vec<String> = match raw {
        Some(value) => parse_csv_list(value)
            .into_iter()
            .map(|s| normalize_scenario(&s))
            .collect(),
        None => DEFAULT_SCENARIOS.iter().map(|s| s.to_string()).collect(),
    };

    for item in source {
        if item.is_empty() {
            continue;
        }
        if seen.insert(item.clone()) {
            out.push(item);
        }
    }
    out.sort_by_key(|scenario| scenario_sort_key(scenario));
    out
}

pub fn scenario_sort_key(name: &str) -> (u8, usize, usize, String) {
    let scenario = normalize_scenario(name);
    if let Some((p, c)) = parse_scenario_threads(&scenario) {
        return (0, p, c, scenario);
    }
    (1, usize::MAX, usize::MAX, scenario)
}

pub fn parse_scenario_threads(scenario: &str) -> Option<(usize, usize)> {
    let normalized = normalize_scenario(scenario);
    let (producer_part, consumer_part_with_c) = normalized.split_once('p')?;
    let consumer_part = consumer_part_with_c.strip_suffix('c')?;
    if producer_part.is_empty() || consumer_part.is_empty() {
        return None;
    }
    if !producer_part.chars().all(|c| c.is_ascii_digit())
        || !consumer_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let producers = producer_part.parse::<usize>().ok()?;
    let consumers = consumer_part.parse::<usize>().ok()?;
    if producers == 0 || consumers == 0 {
        return None;
    }
    Some((producers, consumers))
}

fn collect_run_jsons(runs_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    if !runs_dir.exists() {
        return Ok(files);
    }

    let entries = fs::read_dir(runs_dir)
        .map_err(|err| format!("failed to read runs dir {}: {err}", runs_dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read runs dir entry: {err}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let inner = fs::read_dir(&path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        for item in inner {
            let item = item.map_err(|err| format!("failed to read run file entry: {err}"))?;
            let json_path = item.path();
            if json_path.extension() == Some(OsStr::new("json")) && json_path.is_file() {
                files.push(json_path);
            }
        }
    }
    files.sort();
    Ok(files)
}

pub fn collect_machine_labels(runs_dir: &Path) -> Result<Vec<String>, String> {
    let mut labels = BTreeSet::new();
    for json_path in collect_run_jsons(runs_dir)? {
        let raw = match fs::read_to_string(&json_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let parsed: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let label = parsed
            .get("meta")
            .and_then(|v| v.get("machine_label"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        if !label.is_empty() {
            labels.insert(label.to_string());
        }
    }
    Ok(labels.into_iter().collect())
}

pub fn load_grouped_runs(runs_dir: &Path) -> Result<GroupedRuns, String> {
    let mut samples: BTreeMap<(String, String, String, String), Vec<f64>> = BTreeMap::new();
    let files = collect_run_jsons(runs_dir)?;

    for json_path in files {
        let raw = match fs::read_to_string(&json_path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let parsed: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let meta = parsed
            .get("meta")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let ubq_label = meta
            .get("ubq_label")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .trim()
            .to_string();
        let machine_label = meta
            .get("machine_label")
            .and_then(|v| v.as_str())
            .unwrap_or("local")
            .trim()
            .to_string();
        let machine_label = if machine_label.is_empty() {
            "local".to_string()
        } else {
            machine_label
        };

        let results = parsed
            .get("results")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for rec in results {
            if rec
                .get("skipped_reason")
                .is_some_and(|v| !v.is_null() && !v.as_str().unwrap_or("").is_empty())
            {
                continue;
            }
            let ops = match rec.get("ops_per_sec").and_then(|v| v.as_f64()) {
                Some(v) => v,
                None => continue,
            };
            let queue = rec
                .get("queue")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if queue.is_empty() {
                continue;
            }
            let scenario = normalize_scenario(
                rec.get("scenario")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim(),
            );
            if scenario.is_empty() {
                continue;
            }
            let mode = rec
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("throughput")
                .trim()
                .to_string();
            let queue_label = if queue == "ubq" {
                format!("ubq_{ubq_label}")
            } else {
                queue
            };
            let key = (
                normalize_machine(&machine_label),
                mode,
                scenario,
                queue_label,
            );
            samples.entry(key).or_default().push(ops);
        }
    }

    let mut grouped: GroupedRuns = BTreeMap::new();
    for ((machine, mode, scenario, label), values) in samples {
        if values.is_empty() {
            continue;
        }
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        grouped
            .entry(machine)
            .or_default()
            .entry(mode)
            .or_default()
            .entry(scenario)
            .or_default()
            .insert(
                label,
                Stats {
                    mean_ops_per_sec: mean,
                },
            );
    }
    Ok(grouped)
}

pub fn strict_immediate_winner_ubq_labels(
    entries: &BTreeMap<String, Stats>,
) -> Option<(String, BTreeSet<String>)> {
    let mut parsed: Vec<(String, UbqLabel)> = Vec::new();
    for label in entries.keys() {
        if let Some(raw) = label.strip_prefix("ubq_") {
            if let Ok(parsed_label) = parse_ubq_label(raw, true) {
                parsed.push((label.clone(), parsed_label));
            }
        }
    }
    if parsed.is_empty() {
        return None;
    }

    let winner = parsed
        .iter()
        .max_by(|(l_label, _), (r_label, _)| {
            let l = entries
                .get(l_label)
                .map(|s| s.mean_ops_per_sec)
                .unwrap_or(f64::MIN);
            let r = entries
                .get(r_label)
                .map(|s| s.mean_ops_per_sec)
                .unwrap_or(f64::MIN);
            l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(label, parsed_label)| (label.clone(), parsed_label.clone()))
        .expect("non-empty");

    let winner_label = winner.0;
    let winner_params = winner.1;

    let mut required = BTreeSet::new();
    required.insert(winner_label.clone());

    for idx in 0..4 {
        let neighbors = immediate_neighbors(&winner_params, idx);
        for candidate in neighbors {
            if is_valid_ubq_label(&candidate) {
                required.insert(format!("ubq_{}", candidate.text()));
            }
        }
    }

    let pooled_versions = [3_u8, 4, 5, 7];
    if pooled_versions.contains(&winner_params.version) {
        for version in pooled_versions {
            let candidate = UbqLabel {
                version,
                pool: winner_params.pool,
                block: winner_params.block,
                backoff: winner_params.backoff.clone(),
            };
            if is_valid_ubq_label(&candidate) {
                required.insert(format!("ubq_{}", candidate.text()));
            }
        }
    }

    if winner_params.version == 6 {
        for version in pooled_versions {
            let candidate = UbqLabel {
                version,
                pool: 1,
                block: winner_params.block,
                backoff: winner_params.backoff.clone(),
            };
            if is_valid_ubq_label(&candidate) {
                required.insert(format!("ubq_{}", candidate.text()));
            }
        }
    }

    if winner_params.version == 6 || pooled_versions.contains(&winner_params.version) {
        let candidate = UbqLabel {
            version: 6,
            pool: 0,
            block: winner_params.block,
            backoff: winner_params.backoff.clone(),
        };
        if is_valid_ubq_label(&candidate) {
            required.insert(format!("ubq_{}", candidate.text()));
        }
    }

    Some((winner_label, required))
}

fn immediate_neighbors(label: &UbqLabel, idx: usize) -> Vec<UbqLabel> {
    let mut out = Vec::new();
    match idx {
        0 => {
            for neighbor in immediate_domain_neighbors_u8(label.version, &UBQ_VERSIONS) {
                out.push(UbqLabel {
                    version: neighbor,
                    pool: label.pool,
                    block: label.block,
                    backoff: label.backoff.clone(),
                });
            }
        }
        1 => {
            let pools = [0_u8, 1, 2, 4, 8, 16, 32, 64];
            for neighbor in immediate_domain_neighbors_u8(label.pool, &pools) {
                out.push(UbqLabel {
                    version: label.version,
                    pool: neighbor,
                    block: label.block,
                    backoff: label.backoff.clone(),
                });
            }
        }
        2 => {
            for neighbor in immediate_domain_neighbors_u16(label.block, &UBQ_BLOCK_VALUES) {
                out.push(UbqLabel {
                    version: label.version,
                    pool: label.pool,
                    block: neighbor,
                    backoff: label.backoff.clone(),
                });
            }
        }
        3 => {
            for neighbor in immediate_domain_neighbors_str(&label.backoff, &UBQ_BACKOFF_VALUES) {
                out.push(UbqLabel {
                    version: label.version,
                    pool: label.pool,
                    block: label.block,
                    backoff: neighbor.to_string(),
                });
            }
        }
        _ => {}
    }
    out
}

fn immediate_domain_neighbors_u8(value: u8, domain: &[u8]) -> Vec<u8> {
    if let Some(idx) = domain.iter().position(|v| *v == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

fn immediate_domain_neighbors_u16(value: u16, domain: &[u16]) -> Vec<u16> {
    if let Some(idx) = domain.iter().position(|v| *v == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

fn immediate_domain_neighbors_str<'a>(value: &str, domain: &'a [&str]) -> Vec<&'a str> {
    if let Some(idx) = domain.iter().position(|v| *v == value) {
        let mut out = Vec::new();
        if idx > 0 {
            out.push(domain[idx - 1]);
        }
        if idx + 1 < domain.len() {
            out.push(domain[idx + 1]);
        }
        return out;
    }
    Vec::new()
}

pub fn has_complete_immediate_winner_variants(entries: &BTreeMap<String, Stats>) -> bool {
    let Some((_winner, required)) = strict_immediate_winner_ubq_labels(entries) else {
        return false;
    };
    required.iter().all(|label| entries.contains_key(label))
}

pub fn total_valid_ubq_label_count() -> usize {
    let mut total = 0_usize;
    for version in UBQ_VERSIONS {
        for block in UBQ_BLOCK_VALUES {
            for backoff in UBQ_BACKOFF_VALUES {
                if version == 6 {
                    total += 1;
                    let _ = UbqLabel {
                        version,
                        pool: 0,
                        block,
                        backoff: backoff.to_string(),
                    };
                    continue;
                }
                for pool in UBQ_POOL_VALUES {
                    total += 1;
                    let _ = UbqLabel {
                        version,
                        pool,
                        block,
                        backoff: backoff.to_string(),
                    };
                }
            }
        }
    }
    total
}

pub fn shell_quote(input: &str) -> String {
    if input.is_empty() {
        return "''".to_string();
    }
    let mut out = String::with_capacity(input.len() + 8);
    out.push('\'');
    for ch in input.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

pub fn format_cmd(args: &[String]) -> String {
    let mut out = String::new();
    for (idx, item) in args.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        out.push_str(&shell_quote(item));
    }
    out
}

pub fn parse_bool_token(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub fn validate_forwarded_args(args: &[String], forbidden: &[&str]) -> Result<(), String> {
    for arg in args {
        for key in forbidden {
            if arg == key || arg.starts_with(&format!("{key}=")) {
                return Err(format!(
                    "forwarded arg '{arg}' cannot set protected option '{key}'"
                ));
            }
        }
    }
    Ok(())
}

pub fn parse_key_val_arg(
    raw: &str,
    key: &str,
    args_iter_value: Option<String>,
) -> Result<Option<String>, String> {
    if raw == key {
        return args_iter_value
            .map(Some)
            .ok_or_else(|| format!("{key} requires a value"));
    }
    if let Some(value) = raw.strip_prefix(&format!("{key}=")) {
        return Ok(Some(value.to_string()));
    }
    Ok(None)
}

pub fn join_remote_path(base: &str, child: &str) -> String {
    if child.starts_with('/') || child.starts_with("~/") {
        return child.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        child.trim_start_matches('/')
    )
}

pub fn remote_cd_expr(path: &str) -> String {
    let raw = path.trim();
    if raw == "~" {
        "\"$HOME\"".to_string()
    } else if let Some(tail) = raw.strip_prefix("~/") {
        format!("\"$HOME/{}\"", escape_for_double_quotes(tail))
    } else {
        shell_quote(raw)
    }
}

pub fn escape_for_double_quotes(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}

pub fn normalize_machine_list(raw: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for item in raw.split(',') {
        let value = normalize_machine(item);
        if value.is_empty() {
            continue;
        }
        if seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

pub fn find_missing_machine_labels(requested: &[String], seen: &[String]) -> Vec<String> {
    let seen_norm: BTreeSet<String> = seen.iter().map(|s| normalize_machine(s)).collect();
    requested
        .iter()
        .filter(|m| !seen_norm.contains(&normalize_machine(m)))
        .cloned()
        .collect()
}

pub fn format_missing_key(winner: &str, missing: &[String]) -> String {
    let mut out = String::new();
    let _ = write!(&mut out, "{}|", winner);
    for (idx, label) in missing.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(label);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn ubq_label_parse_and_normalize() {
        let parsed = parse_ubq_label("v5,8,2047,b", true).expect("parse");
        assert_eq!(parsed.version, 5);
        assert_eq!(parsed.pool, 8);
        assert_eq!(parsed.block, 2047);
        assert_eq!(parsed.backoff, "b");
        assert_eq!(parsed.text(), "v5,8,2047,b");
        assert_eq!(parsed.safe(), "v5_8_2047_b");
        assert_eq!(
            parsed.feature_csv(),
            "ubq_v5,ubq_pool_8,ubq_block_2047,ubq_backoff_cq"
        );
        assert!(normalize_ubq_label("v6,0,1023", true).is_some());
        assert!(normalize_ubq_label("v6,8,1023", true).is_none());
    }

    #[test]
    fn immediate_neighbors_cover_v6_and_pooled_variants() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "ubq_v5,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 120.0,
            },
        );
        entries.insert(
            "ubq_v4,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 80.0,
            },
        );
        entries.insert(
            "ubq_v6,0,1023".to_string(),
            Stats {
                mean_ops_per_sec: 60.0,
            },
        );
        let (winner, required) = strict_immediate_winner_ubq_labels(&entries).expect("winner");
        assert_eq!(winner, "ubq_v5,1,1023");
        assert!(required.contains("ubq_v6,0,1023"));
        assert!(required.contains("ubq_v4,1,1023"));
        assert!(required.contains("ubq_v3,1,1023"));
        assert!(required.contains("ubq_v7,1,1023"));
    }

    #[test]
    fn pooled_winner_requires_v6_at_same_block_for_any_pool() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "ubq_v7,16,511".to_string(),
            Stats {
                mean_ops_per_sec: 200.0,
            },
        );
        entries.insert(
            "ubq_v5,16,511".to_string(),
            Stats {
                mean_ops_per_sec: 150.0,
            },
        );
        let (_winner, required) = strict_immediate_winner_ubq_labels(&entries).expect("winner");
        assert!(required.contains("ubq_v6,0,511"));
    }

    #[test]
    fn complete_variants_detection() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "ubq_v4,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 120.0,
            },
        );
        entries.insert(
            "ubq_v3,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 110.0,
            },
        );
        entries.insert(
            "ubq_v5,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 100.0,
            },
        );
        entries.insert(
            "ubq_v7,1,1023".to_string(),
            Stats {
                mean_ops_per_sec: 90.0,
            },
        );
        entries.insert(
            "ubq_v6,0,1023".to_string(),
            Stats {
                mean_ops_per_sec: 95.0,
            },
        );
        entries.insert(
            "ubq_v4,2,1023".to_string(),
            Stats {
                mean_ops_per_sec: 80.0,
            },
        );
        entries.insert(
            "ubq_v4,1,511".to_string(),
            Stats {
                mean_ops_per_sec: 70.0,
            },
        );
        entries.insert(
            "ubq_v4,1,2047".to_string(),
            Stats {
                mean_ops_per_sec: 60.0,
            },
        );
        entries.insert(
            "ubq_v4,1,1023,b".to_string(),
            Stats {
                mean_ops_per_sec: 50.0,
            },
        );
        assert!(has_complete_immediate_winner_variants(&entries));
    }

    #[test]
    fn command_helper_generation() {
        assert_eq!(
            join_remote_path("~/UBQ", "bench_results/runs"),
            "~/UBQ/bench_results/runs"
        );
        assert_eq!(join_remote_path("~/UBQ", "/tmp/runs"), "/tmp/runs");
        assert_eq!(remote_cd_expr("~/UBQ"), "\"$HOME/UBQ\"");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn aggregate_runs_layout_compat() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("ubq_tooling_test_{stamp}"));
        let run_dir = root.join("v4_8_127");
        fs::create_dir_all(&run_dir).expect("mkdir");
        let payload = serde_json::json!({
            "meta": {
                "ubq_label": "v4,8,127",
                "machine_label": "local"
            },
            "results": [
                {"queue":"ubq","scenario":"1p1c","mode":"throughput","ops_per_sec":100.0},
                {"queue":"segqueue","scenario":"1p1c","mode":"throughput","ops_per_sec":90.0}
            ]
        });
        fs::write(
            run_dir.join("local_1.json"),
            serde_json::to_string_pretty(&payload).expect("json"),
        )
        .expect("write");

        let grouped = load_grouped_runs(&root).expect("group");
        let entries = grouped
            .get("local")
            .and_then(|m| m.get("throughput"))
            .and_then(|m| m.get("1p1c"))
            .expect("entries");
        assert!(entries.contains_key("ubq_v4,8,127"));
        assert!(entries.contains_key("segqueue"));

        let labels = collect_machine_labels(&root).expect("labels");
        assert_eq!(labels, vec!["local".to_string()]);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ubq_label_domain_size_is_finite_and_expected() {
        assert_eq!(total_valid_ubq_label_count(), 464);
    }
}

fn main() {
    unimplemented!()
}
