//! Turns raw GitHub webhook payloads into Taggle's normalized activity rows.
//!
//! Private repositories are masked by default: repo name, titles, messages, branch and
//! URLs are dropped and `private: true` is set, so only "something happened" leaks.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};

pub struct Act {
    pub kind: &'static str,
    pub key: String,
    /// Fields present on first insert only (enrichment placeholders).
    pub defaults: Value,
    /// Fields refreshed on every event.
    pub base: Value,
    pub at: DateTime<Utc>,
}

pub fn s<'a>(v: &'a Value, ptr: &str) -> Option<&'a str> {
    v.pointer(ptr).and_then(Value::as_str)
}

fn ts(v: &Value, ptr: &str) -> DateTime<Utc> {
    s(v, ptr)
        .and_then(|x| DateTime::parse_from_rfc3339(x).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}

pub fn person(login: &str) -> Value {
    json!({ "username": login, "avatar_url": format!("https://github.com/{login}.png") })
}

fn is_private(p: &Value) -> bool {
    p.pointer("/repository/private").and_then(Value::as_bool).unwrap_or(false)
}

fn repo_name(p: &Value) -> Option<String> {
    s(p, "/repository/full_name").map(str::to_owned)
}

/// Keeps `value` unless the repo is private.
fn public_only(private: bool, value: Value) -> Value {
    if private { Value::Null } else { value }
}

pub fn from_event(event: &str, p: &Value) -> Vec<Act> {
    match event {
        "push" => push(p),
        "pull_request" | "pull_request_review" | "pull_request_review_thread" | "pull_request_review_comment" => {
            p.get("pull_request").and_then(|pr| pull_request(p, pr)).into_iter().collect()
        }
        "issues" => p.get("issue").and_then(|i| issue(p, i)).into_iter().collect(),
        // Comments on PRs arrive as issue_comment; those only need enrichment, not an issue row.
        "issue_comment" => match p.get("issue") {
            Some(i) if i.get("pull_request").is_none() => issue(p, i).into_iter().collect(),
            _ => vec![],
        },
        _ => vec![],
    }
}

fn push(p: &Value) -> Vec<Act> {
    if p.get("deleted").and_then(Value::as_bool).unwrap_or(false) {
        return vec![];
    }
    let (Some(repo), Some(git_ref)) = (repo_name(p), s(p, "/ref")) else { return vec![] };
    let branch = git_ref.strip_prefix("refs/heads/").unwrap_or(git_ref);
    let private = is_private(p);

    p.get("commits")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(20)
        .filter_map(|c| {
            let sha = s(c, "/id")?;
            let short = &sha[..sha.len().min(7)];
            let author = s(c, "/author/username").or_else(|| s(c, "/committer/username"));
            let message = s(c, "/message").map(|m| m.lines().next().unwrap_or("").to_owned());
            Some(Act {
                kind: "commit",
                key: format!("{repo}@{sha}"),
                defaults: Value::Null,
                base: json!({
                    "sha": short,
                    "message": public_only(private, json!(message)),
                    "repo": public_only(private, json!(repo)),
                    "branch": public_only(private, json!(branch)),
                    "private": private,
                    "author": public_only(private, json!(author.map(person))),
                    "url": public_only(private, json!(s(c, "/url"))),
                    "timestamp": ts(c, "/timestamp"),
                }),
                at: ts(c, "/timestamp"),
            })
        })
        .collect()
}

fn pull_request(p: &Value, pr: &Value) -> Option<Act> {
    let repo = repo_name(p)?;
    let number = pr.get("number")?.as_i64()?;
    let private = is_private(p);
    let merged = pr.get("merged").and_then(Value::as_bool).unwrap_or(false);
    let state = if merged { "merged" } else { s(pr, "/state").unwrap_or("open") };
    let at = ts(pr, "/updated_at");

    Some(Act {
        kind: "pull_request",
        key: format!("{repo}#{number}"),
        defaults: json!({
            "checks_status": null,
            "deployment_status": null,
            "review_threads": null,
            "participants": [],
        }),
        base: json!({
            "id": number,
            "number": number,
            "title": public_only(private, json!(s(pr, "/title"))),
            "repo": public_only(private, json!(repo)),
            "branch": public_only(private, json!(s(pr, "/head/ref"))),
            "state": state,
            "draft": pr.get("draft").and_then(Value::as_bool).unwrap_or(false),
            "private": private,
            "author": public_only(private, json!(s(pr, "/user/login").map(person))),
            "url": public_only(private, json!(s(pr, "/html_url"))),
            "updated_at": at,
        }),
        at,
    })
}

fn issue(p: &Value, i: &Value) -> Option<Act> {
    let repo = repo_name(p)?;
    let number = i.get("number")?.as_i64()?;
    let private = is_private(p);
    let at = ts(i, "/updated_at");

    let labels: Vec<Value> = i
        .get("labels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|l| json!({ "name": s(l, "/name"), "color": s(l, "/color") }))
        .collect();

    Some(Act {
        kind: "issue",
        key: format!("{repo}#{number}"),
        defaults: Value::Null,
        base: json!({
            "number": number,
            "title": public_only(private, json!(s(i, "/title"))),
            "repo": public_only(private, json!(repo)),
            "state": s(i, "/state").unwrap_or("open"),
            "labels": if private { json!([]) } else { Value::Array(labels) },
            "comments": i.get("comments").and_then(Value::as_i64).unwrap_or(0),
            "private": private,
            "author": public_only(private, json!(s(i, "/user/login").map(person))),
            "url": public_only(private, json!(s(i, "/html_url"))),
            "updated_at": at,
        }),
        at,
    })
}

/// Pull requests whose enrichment (review threads, checks) should be refreshed.
/// Returns (owner, repo, number).
pub fn pr_targets(event: &str, p: &Value) -> Vec<(String, String, i64)> {
    let Some(full) = s(p, "/repository/full_name") else { return vec![] };
    let Some((owner, repo)) = full.split_once('/') else { return vec![] };

    let numbers: Vec<i64> = match event {
        "pull_request" | "pull_request_review" | "pull_request_review_thread" | "pull_request_review_comment" => {
            p.pointer("/pull_request/number").and_then(Value::as_i64).into_iter().collect()
        }
        "issue_comment" if p.pointer("/issue/pull_request").is_some() => {
            p.pointer("/issue/number").and_then(Value::as_i64).into_iter().collect()
        }
        "check_run" | "check_suite" => p
            .pointer(&format!("/{}/pull_requests", event))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|x| x.get("number").and_then(Value::as_i64))
            .take(10)
            .collect(),
        _ => vec![],
    };
    numbers.into_iter().map(|n| (owner.to_owned(), repo.to_owned(), n)).collect()
}

/// Shallow-merges `b` over `a` (both objects). Non-objects: `b` wins unless null.
pub fn merge(a: &Value, b: &Value) -> Value {
    let mut out: Map<String, Value> = a.as_object().cloned().unwrap_or_default();
    if let Some(bm) = b.as_object() {
        for (k, v) in bm {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_public_push_and_masks_private() {
        let public_push = json!({
            "ref": "refs/heads/main",
            "repository": { "full_name": "example-user/demo-repo", "private": false },
            "commits": [{
                "id": "abcdef1234567890",
                "message": "feat: ws\n\ndetails",
                "timestamp": "2026-11-11T23:00:00Z",
                "url": "https://github.com/example-user/demo-repo/commit/abcdef1234567890",
                "author": { "username": "example-user" }
            }]
        });
        let acts = from_event("push", &public_push);
        assert_eq!(acts.len(), 1);
        assert_eq!(acts[0].kind, "commit");
        assert_eq!(acts[0].base["sha"], "abcdef1");
        assert_eq!(acts[0].base["message"], "feat: ws");
        assert_eq!(acts[0].base["repo"], "example-user/demo-repo");
        assert_eq!(acts[0].base["branch"], "main");
        assert_eq!(acts[0].base["private"], false);

        let private_push = json!({
            "ref": "refs/heads/secret-branch",
            "repository": { "full_name": "example-user/private-repo", "private": true },
            "commits": [{
                "id": "1234567890",
                "message": "top secret fix",
                "timestamp": "2026-11-11T23:00:00Z",
                "url": "https://github.com/example-user/private-repo/commit/1234567890",
                "author": { "username": "example-user" }
            }]
        });
        let p_acts = from_event("push", &private_push);
        assert_eq!(p_acts.len(), 1);
        assert_eq!(p_acts[0].key, "example-user/private-repo@1234567890");
        assert_eq!(p_acts[0].base["private"], true);
        assert!(p_acts[0].base["repo"].is_null());
        assert!(p_acts[0].base["message"].is_null());
        assert!(p_acts[0].base["branch"].is_null());
        assert!(p_acts[0].base["author"].is_null());
        assert!(p_acts[0].base["url"].is_null());
    }

    #[test]
    fn normalizes_pr_and_skips_pr_issue_comment_row() {
        let pr_ev = json!({
            "repository": { "full_name": "example-user/demo-repo", "private": false },
            "pull_request": {
                "number": 42,
                "title": "Add socket",
                "state": "closed",
                "merged": true,
                "draft": false,
                "head": { "ref": "feat/ws" },
                "user": { "login": "example-user" },
                "html_url": "https://github.com/example-user/demo-repo/pull/42",
                "updated_at": "2026-11-11T23:05:00Z"
            }
        });
        let acts = from_event("pull_request", &pr_ev);
        assert_eq!(acts.len(), 1);
        assert_eq!(acts[0].base["state"], "merged");
        assert_eq!(acts[0].base["branch"], "feat/ws");

        let pr_comment = json!({
            "repository": { "full_name": "example-user/demo-repo", "private": false },
            "issue": { "number": 42, "pull_request": { "url": "..." } }
        });
        assert!(from_event("issue_comment", &pr_comment).is_empty());
        assert_eq!(pr_targets("issue_comment", &pr_comment), vec![("example-user".into(), "demo-repo".into(), 42)]);
    }
}
