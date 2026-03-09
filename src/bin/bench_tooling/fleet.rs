use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

pub fn normalize_machine(name: &str) -> String {
    name.trim().to_ascii_lowercase()
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
