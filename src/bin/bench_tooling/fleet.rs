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

    collect_run_jsons_recursive(runs_dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_run_jsons_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|err| format!("failed to read runs dir {}: {err}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read runs dir entry: {err}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_run_jsons_recursive(&path, files)?;
            continue;
        }
        if path.extension() == Some(OsStr::new("json")) && path.is_file() {
            files.push(path);
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn collect_machine_labels_reads_machine_first_layout() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("ubq_fleet_tooling_test_{stamp}"));
        let local_dir = root.join("local").join("v4_8_127");
        let lab_dir = root.join("lab").join("v5_4_1023");
        fs::create_dir_all(&local_dir).expect("mkdir local");
        fs::create_dir_all(&lab_dir).expect("mkdir lab");

        let local_payload = serde_json::json!({
            "meta": { "machine_label": "local" },
            "results": []
        });
        let lab_payload = serde_json::json!({
            "meta": { "machine_label": "lab" },
            "results": []
        });
        fs::write(
            local_dir.join("1773004334181.json"),
            serde_json::to_string_pretty(&local_payload).expect("json"),
        )
        .expect("write local");
        fs::write(
            lab_dir.join("1773004335123.json"),
            serde_json::to_string_pretty(&lab_payload).expect("json"),
        )
        .expect("write lab");

        let labels = collect_machine_labels(&root).expect("labels");
        assert_eq!(labels, vec!["lab".to_string(), "local".to_string()]);

        let _ = fs::remove_dir_all(&root);
    }
}
