//! GitHub App authentication, GraphQL enrichment, and automatic commit/PR/issue sync.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::{Value, json};

use crate::normalize::{Act, person};

const API: &str = "https://api.github.com";

pub struct GitHub {
    http: reqwest::Client,
    app_id: String,
    key: EncodingKey,
    tokens: Mutex<HashMap<i64, (String, Instant)>>,
    etags: Mutex<HashMap<String, String>>,
    unauth_core_reset_at: Mutex<Option<Instant>>,
    unauth_search_reset_at: Mutex<Option<Instant>>,
    repo_pushed_at: Mutex<HashMap<String, String>>,
    inst_pushed_at: Mutex<HashMap<String, String>>,
    last_search_at: Mutex<HashMap<String, Instant>>,
}

#[derive(Serialize)]
struct Claims<'a> {
    iat: u64,
    exp: u64,
    iss: &'a str,
}

pub struct AppInstallation {
    pub id: i64,
    pub login: String,
    pub account_id: i64,
    pub avatar_url: Option<String>,
}

pub struct Enrichment {
    pub review_threads: Value,
    pub checks_status: Value,
    pub participants: Value,
    pub merged: Option<bool>,
}

impl GitHub {
    pub fn new(app_id: &str, pem: &str) -> Result<Self> {
        // Infisical/env may store the key with literal "\n" sequences.
        let pem = pem.replace("\\n", "\n");
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent("taggle-ingest")
                .timeout(Duration::from_secs(15))
                .build()?,
            app_id: app_id.to_owned(),
            key: EncodingKey::from_rsa_pem(pem.as_bytes()).context("invalid GITHUB_APP_PRIVATE_KEY")?,
            tokens: Mutex::new(HashMap::new()),
            etags: Mutex::new(HashMap::new()),
            unauth_core_reset_at: Mutex::new(None),
            unauth_search_reset_at: Mutex::new(None),
            repo_pushed_at: Mutex::new(HashMap::new()),
            inst_pushed_at: Mutex::new(HashMap::new()),
            last_search_at: Mutex::new(HashMap::new()),
        })
    }

    fn app_jwt(&self) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let claims = Claims { iat: now - 60, exp: now + 9 * 60, iss: &self.app_id };
        Ok(encode(&Header::new(Algorithm::RS256), &claims, &self.key)?)
    }

    async fn installation_token(&self, installation: i64) -> Result<String> {
        if let Some((tok, exp)) = self.tokens.lock().unwrap().get(&installation) {
            if *exp > Instant::now() {
                return Ok(tok.clone());
            }
        }
        let resp = self
            .http
            .post(format!("{API}/app/installations/{installation}/access_tokens"))
            .bearer_auth(self.app_jwt()?)
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("installation token request failed: {}", resp.status());
        }
        let body: Value = resp.json().await?;
        let token = body["token"].as_str().context("no token in response")?.to_owned();
        // Tokens live 1h; refresh after 50 minutes.
        self.tokens
            .lock()
            .unwrap()
            .insert(installation, (token.clone(), Instant::now() + Duration::from_secs(50 * 60)));
        Ok(token)
    }

    async fn get_json(&self, url: &str, bearer: Option<&str>, use_etag: bool) -> Result<Option<Value>> {
        let is_search = url.contains("/search/");
        if bearer.is_none() {
            let cooldown = if is_search {
                &self.unauth_search_reset_at
            } else {
                &self.unauth_core_reset_at
            };
            if let Some(until) = *cooldown.lock().unwrap() {
                if Instant::now() < until {
                    bail!("github public rate limit cooling down");
                }
            }
        }

        let mut req = self
            .http
            .get(url)
            .header("Accept", "application/vnd.github+json");
        if let Some(tok) = bearer {
            req = req.bearer_auth(tok);
        }
        if use_etag && bearer.is_none() {
            if let Some(etag) = self.etags.lock().unwrap().get(url).cloned() {
                req = req.header("If-None-Match", etag);
            }
        }

        let resp = req
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("github request failed: {}", err.without_url()))?;
        let status = resp.status();

        if status == StatusCode::NOT_MODIFIED {
            return Ok(None);
        }
        if status == StatusCode::CONFLICT {
            // Empty repositories return 409 on /commits.
            return Ok(Some(json!([])));
        }

        if matches!(status, StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS) {
            let remaining = resp
                .headers()
                .get("x-ratelimit-remaining")
                .and_then(|v| v.to_str().ok());
            if bearer.is_none() && (remaining == Some("0") || status == StatusCode::TOO_MANY_REQUESTS) {
                let wait_secs = resp
                    .headers()
                    .get("x-ratelimit-reset")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .and_then(|reset| {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                        Some(reset.saturating_sub(now).clamp(30, 900))
                    })
                    .unwrap_or(120);
                let target = if is_search {
                    &self.unauth_search_reset_at
                } else {
                    &self.unauth_core_reset_at
                };
                *target.lock().unwrap() = Some(Instant::now() + Duration::from_secs(wait_secs));
            }
            bail!("github GET {url} failed: {status}");
        }

        if !status.is_success() {
            bail!("github GET {url} failed: {status}");
        }

        if use_etag
            && bearer.is_none()
            && let Some(etag) = resp.headers().get("etag").and_then(|v| v.to_str().ok())
        {
            let mut map = self.etags.lock().unwrap();
            if map.len() >= 2000 {
                map.clear();
            }
            map.insert(url.to_owned(), etag.to_owned());
        }

        Ok(Some(resp.json().await?))
    }

    async fn graphql(&self, installation: i64, query: &str, variables: Value) -> Result<Value> {
        let token = self.installation_token(installation).await?;
        let resp = self
            .http
            .post(format!("{API}/graphql"))
            .bearer_auth(token)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("graphql request failed: {}", resp.status());
        }
        Ok(resp.json().await?)
    }

    pub async fn fetch_app_slug(&self) -> Result<String> {
        let jwt = self.app_jwt()?;
        let body = self
            .get_json(&format!("{API}/app"), Some(&jwt), false)
            .await?
            .context("empty /app response")?;
        body["slug"].as_str().map(str::to_owned).context("missing slug in /app response")
    }

    pub async fn list_app_installations(&self) -> Result<Vec<AppInstallation>> {
        let jwt = self.app_jwt()?;
        let Some(body) = self
            .get_json(&format!("{API}/app/installations?per_page=100"), Some(&jwt), false)
            .await?
        else {
            return Ok(vec![]);
        };
        Ok(body
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| {
                let id = item.get("id")?.as_i64()?;
                let login = item.pointer("/account/login")?.as_str()?.to_owned();
                let account_id = item.pointer("/account/id")?.as_i64()?;
                let avatar_url = item
                    .pointer("/account/avatar_url")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                Some(AppInstallation { id, login, account_id, avatar_url })
            })
            .collect())
    }

    pub async fn enrich_pr(&self, installation: i64, owner: &str, repo: &str, number: i64) -> Result<Enrichment> {
        const QUERY: &str = r#"
        query($owner:String!,$name:String!,$number:Int!){
          repository(owner:$owner,name:$name){
            pullRequest(number:$number){
              merged
              reviewThreads(first:100){ totalCount nodes{ isResolved } }
              commits(last:1){ nodes{ commit{ statusCheckRollup{ state } } } }
              reviews(first:30){ nodes{ author{ login } } }
            }
          }
        }"#;

        let body = self
            .graphql(installation, QUERY, json!({ "owner": owner, "name": repo, "number": number }))
            .await?;
        let pr = body.pointer("/data/repository/pullRequest").filter(|v| !v.is_null());
        let Some(pr) = pr else {
            bail!("pull request not found in graphql response: {}", body["errors"])
        };
        Ok(parse_enrichment(pr))
    }

    /// Fetches up to 6 most recently pushed accessible repos and their commits, PRs and issues.
    pub async fn backfill_installation(&self, installation: i64) -> Result<Vec<Act>> {
        let token = self.installation_token(installation).await?;
        let Some(body) = self
            .get_json(&format!("{API}/installation/repositories?per_page=100"), Some(&token), false)
            .await?
        else {
            return Ok(vec![]);
        };
        let mut repos: Vec<&Value> = body
            .get("repositories")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .collect();
        repos.sort_by(|a, b| {
            let pa = a.get("pushed_at").and_then(Value::as_str).unwrap_or("");
            let pb = b.get("pushed_at").and_then(Value::as_str).unwrap_or("");
            pb.cmp(pa)
        });

        let mut acts = Vec::new();
        for r in repos.into_iter().take(6) {
            let Some(full) = r.get("full_name").and_then(Value::as_str) else { continue };
            if !valid_repo_slug(full) {
                continue;
            }
            let pushed_at = r.get("pushed_at").and_then(Value::as_str).unwrap_or("");
            let cache_key = format!("{installation}:{full}");
            if !pushed_at.is_empty()
                && self.inst_pushed_at.lock().unwrap().get(&cache_key).map(String::as_str) == Some(pushed_at)
            {
                continue;
            }
            let Some((owner, name)) = full.split_once('/') else { continue };
            match self.backfill_repo(installation, owner, name).await {
                Ok(mut repo_acts) => {
                    if !pushed_at.is_empty() {
                        let mut map = self.inst_pushed_at.lock().unwrap();
                        if map.len() >= 1000 {
                            map.clear();
                        }
                        map.insert(cache_key, pushed_at.to_owned());
                    }
                    acts.append(&mut repo_acts);
                }
                Err(err) => tracing::warn!(error = %err, repo = full, "repo backfill failed"),
            }
        }
        Ok(acts)
    }

    /// Fetches public commits, pull requests and issues for a monitored user without webhooks.
    pub async fn backfill_user_public(&self, login: &str, oauth_token: Option<&str>, use_etag: bool) -> Result<Vec<Act>> {
        if !crate::store::valid_github_login(login) {
            bail!("invalid GitHub login");
        }
        let mut acts = Vec::new();
        let mut seen_keys: HashSet<String> = HashSet::new();

        let repos_url = format!("{API}/users/{login}/repos?type=all&sort=pushed&per_page=6");
        let events_url = format!("{API}/users/{login}/events/public?per_page=20");
        let (repos_res, events_res) = tokio::join!(
            self.get_json(&repos_url, oauth_token, use_etag),
            self.get_json(&events_url, oauth_token, use_etag),
        );

        let any_new_activity = matches!(repos_res, Ok(Some(_))) || matches!(events_res, Ok(Some(_)));
        let search_due = if !use_etag || oauth_token.is_some() {
            true
        } else if any_new_activity {
            let now = Instant::now();
            let last = self.last_search_at.lock().unwrap().get(login).copied();
            last.is_none_or(|prev| now.duration_since(prev) >= Duration::from_secs(180))
        } else {
            false
        };

        let (search1_res, search2_res) = if search_due {
            if oauth_token.is_none() {
                let mut map = self.last_search_at.lock().unwrap();
                if map.len() >= 1000 {
                    map.clear();
                }
                map.insert(login.to_owned(), Instant::now());
            }
            let search_involves = format!("{API}/search/issues?q=involves:{login}+is:public&sort=updated&order=desc&per_page=10");
            let search_user = format!("{API}/search/issues?q=user:{login}+is:public&sort=updated&order=desc&per_page=10");
            tokio::join!(
                self.get_json(&search_involves, oauth_token, use_etag),
                self.get_json(&search_user, oauth_token, use_etag),
            )
        } else {
            (Ok(None), Ok(None))
        };

        // 1. Recent commits from the user's most recently pushed public repos.
        match repos_res {
            Ok(Some(repos_val)) => {
                let public_repos: Vec<(String, String, String)> = repos_val
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|r| !r.get("private").and_then(Value::as_bool).unwrap_or(false))
                    .filter_map(|r| {
                        let full = r.get("full_name")?.as_str()?.to_owned();
                        if !valid_repo_slug(&full) {
                            return None;
                        }
                        let branch = r
                            .get("default_branch")
                            .and_then(Value::as_str)
                            .unwrap_or("main")
                            .to_owned();
                        let pushed_at = r
                            .get("pushed_at")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned();
                        if use_etag
                            && !pushed_at.is_empty()
                            && self.repo_pushed_at.lock().unwrap().get(&full).map(String::as_str) == Some(pushed_at.as_str())
                        {
                            return None;
                        }
                        Some((full, branch, pushed_at))
                    })
                    .take(5)
                    .collect();

                let mut commit_fetches = stream::iter(public_repos.into_iter().map(|(full, branch, pushed_at)| async move {
                    let commits_url = format!("{API}/repos/{full}/commits?per_page=5");
                    let res = self.get_json(&commits_url, oauth_token, use_etag).await;
                    (full, branch, pushed_at, res)
                }))
                .buffer_unordered(4);

                while let Some((full, branch, pushed_at, res)) = commit_fetches.next().await {
                    match res {
                        Ok(Some(commits_val)) => {
                            if !pushed_at.is_empty() {
                                let mut map = self.repo_pushed_at.lock().unwrap();
                                if map.len() >= 2000 {
                                    map.clear();
                                }
                                map.insert(full.clone(), pushed_at);
                            }
                            for c in commits_val.as_array().into_iter().flatten() {
                                if let Some(act) = rest_commit_to_act(&full, Some(&branch), c)
                                    && seen_keys.insert(act.key.clone())
                                {
                                    acts.push(act);
                                }
                            }
                        }
                        Ok(None) => {
                            if !pushed_at.is_empty() {
                                self.repo_pushed_at.lock().unwrap().insert(full, pushed_at);
                            }
                        }
                        Err(err) => tracing::debug!(error = %err, repo = %full, "public commits fetch skipped"),
                    }
                }
            }
            Ok(None) => {}
            Err(err) => tracing::debug!(error = %err, login, "public repos fetch skipped"),
        }

        // 2. PushEvents to pick up non-default branch or external repo commits.
        match events_res {
            Ok(Some(events_val)) => {
                let push_candidates: Vec<(String, String, Option<String>)> = events_val
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|ev| ev.get("type").and_then(Value::as_str) == Some("PushEvent"))
                    .filter_map(|ev| {
                        let repo = ev.pointer("/repo/name")?.as_str()?;
                        let head = ev.pointer("/payload/head")?.as_str()?;
                        if !valid_repo_slug(repo) || head.is_empty() || !head.bytes().all(|b| b.is_ascii_hexdigit()) {
                            return None;
                        }
                        let key = format!("{repo}@{head}");
                        if seen_keys.contains(&key) {
                            return None;
                        }
                        let git_ref = ev.pointer("/payload/ref").and_then(Value::as_str).unwrap_or("");
                        let branch = git_ref
                            .strip_prefix("refs/heads/")
                            .filter(|b| !b.is_empty())
                            .map(str::to_owned);
                        Some((repo.to_owned(), head.to_owned(), branch))
                    })
                    .take(3)
                    .collect();

                let mut push_fetches = stream::iter(push_candidates.into_iter().map(|(repo, head, branch)| async move {
                    let commit_url = format!("{API}/repos/{repo}/commits/{head}");
                    let res = self.get_json(&commit_url, oauth_token, use_etag).await;
                    (repo, branch, res)
                }))
                .buffer_unordered(3);

                while let Some((repo, branch, res)) = push_fetches.next().await {
                    if let Ok(Some(c)) = res
                        && let Some(act) = rest_commit_to_act(&repo, branch.as_deref(), &c)
                        && seen_keys.insert(act.key.clone())
                    {
                        acts.push(act);
                    }
                }
            }
            Ok(None) => {}
            Err(err) => tracing::debug!(error = %err, login, "public events fetch skipped"),
        }

        // 3. Pull requests and issues involving or inside the user's public repos.
        let mut pr_targets: Vec<(String, i64)> = Vec::new();
        for search_res in [search1_res, search2_res] {
            match search_res {
                Ok(Some(search_val)) => {
                    for item in search_val.get("items").and_then(Value::as_array).into_iter().flatten() {
                        if let Some(act) = search_item_to_act(item) {
                            let dedupe_key = format!("{}:{}", act.kind, act.key);
                            if seen_keys.insert(dedupe_key) {
                                if act.kind == "pull_request"
                                    && let (Some(repo), Some(num)) = (
                                        act.base.get("repo").and_then(Value::as_str),
                                        act.base.get("number").and_then(Value::as_i64),
                                    )
                                {
                                    pr_targets.push((repo.to_owned(), num));
                                }
                                acts.push(act);
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => tracing::debug!(error = %err, login, "search issues skipped"),
            }
        }

        // Enrich top recent public PRs with head branch + draft state in parallel.
        let valid_prs: Vec<(String, i64)> = pr_targets
            .into_iter()
            .filter(|(repo, num)| valid_repo_slug(repo) && *num > 0)
            .take(5)
            .collect();

        let mut pr_fetches = stream::iter(valid_prs.into_iter().map(|(repo, num)| async move {
            let pr_url = format!("{API}/repos/{repo}/pulls/{num}");
            let res = self.get_json(&pr_url, oauth_token, use_etag).await;
            (repo, num, res)
        }))
        .buffer_unordered(4);

        while let Some((repo, num, res)) = pr_fetches.next().await {
            if let Ok(Some(pr_val)) = res
                && let Some(act) = acts.iter_mut().find(|a| a.kind == "pull_request" && a.key == format!("{repo}#{num}"))
                && let Some(obj) = act.base.as_object_mut()
            {
                if let Some(branch) = pr_val.pointer("/head/ref").and_then(Value::as_str) {
                    obj.insert("branch".into(), json!(branch));
                }
                if let Some(draft) = pr_val.get("draft").and_then(Value::as_bool) {
                    obj.insert("draft".into(), json!(draft));
                }
                if pr_val.get("merged").and_then(Value::as_bool) == Some(true) {
                    obj.insert("state".into(), json!("merged"));
                }
            }
        }

        Ok(acts)
    }

    async fn backfill_repo(&self, installation: i64, owner: &str, name: &str) -> Result<Vec<Act>> {
        const QUERY: &str = r#"
        query($owner:String!,$name:String!){
          repository(owner:$owner,name:$name){
            nameWithOwner
            isPrivate
            defaultBranchRef{
              name
              target{
                ... on Commit{
                  history(first:5){
                    nodes{ oid abbreviatedOid messageHeadline committedDate url author{ user{ login } } }
                  }
                }
              }
            }
            pullRequests(first:5,orderBy:{field:UPDATED_AT,direction:DESC}){
              nodes{
                number title state isDraft merged updatedAt url headRefName
                author{ login }
                reviewThreads(first:100){ totalCount nodes{ isResolved } }
                commits(last:1){ nodes{ commit{ statusCheckRollup{ state } } } }
                reviews(first:30){ nodes{ author{ login } } }
              }
            }
            issues(first:5,orderBy:{field:UPDATED_AT,direction:DESC}){
              nodes{
                number title state updatedAt url
                author{ login }
                comments{ totalCount }
                labels(first:10){ nodes{ name color } }
              }
            }
          }
        }"#;

        let body = self.graphql(installation, QUERY, json!({ "owner": owner, "name": name })).await?;
        let Some(repo) = body.pointer("/data/repository").filter(|v| !v.is_null()) else {
            return Ok(vec![]);
        };
        let full = repo["nameWithOwner"].as_str().unwrap_or("");
        let private = repo["isPrivate"].as_bool().unwrap_or(false);
        let mask = |v: Value| if private { Value::Null } else { v };
        let mut acts = Vec::new();

        let branch = repo.pointer("/defaultBranchRef/name").and_then(Value::as_str);
        for c in repo
            .pointer("/defaultBranchRef/target/history/nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(oid) = c["oid"].as_str() else { continue };
            let at = parse_ts(c["committedDate"].as_str());
            let author = c.pointer("/author/user/login").and_then(Value::as_str);
            acts.push(Act {
                kind: "commit",
                key: format!("{full}@{oid}"),
                defaults: Value::Null,
                base: json!({
                    "sha": c["abbreviatedOid"].as_str().unwrap_or(&oid[..oid.len().min(7)]),
                    "message": mask(c["messageHeadline"].clone()),
                    "repo": mask(json!(full)),
                    "branch": mask(json!(branch)),
                    "private": private,
                    "author": mask(json!(author.map(person))),
                    "url": mask(c["url"].clone()),
                    "timestamp": at,
                }),
                at,
            });
        }

        for pr in repo
            .pointer("/pullRequests/nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(number) = pr["number"].as_i64() else { continue };
            let enrich = parse_enrichment(pr);
            let at = parse_ts(pr["updatedAt"].as_str());
            let state = if pr["merged"].as_bool() == Some(true) {
                "merged"
            } else {
                match pr["state"].as_str() {
                    Some("CLOSED") => "closed",
                    _ => "open",
                }
            };
            acts.push(Act {
                kind: "pull_request",
                key: format!("{full}#{number}"),
                defaults: json!({
                    "review_threads": enrich.review_threads,
                    "checks_status": enrich.checks_status,
                    "deployment_status": null,
                    "participants": if private { json!([]) } else { enrich.participants },
                }),
                base: json!({
                    "id": number,
                    "number": number,
                    "title": mask(pr["title"].clone()),
                    "repo": mask(json!(full)),
                    "branch": mask(pr["headRefName"].clone()),
                    "state": state,
                    "draft": pr["isDraft"].as_bool().unwrap_or(false),
                    "private": private,
                    "author": mask(json!(pr.pointer("/author/login").and_then(Value::as_str).map(person))),
                    "url": mask(pr["url"].clone()),
                    "updated_at": at,
                }),
                at,
            });
        }

        for i in repo
            .pointer("/issues/nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(number) = i["number"].as_i64() else { continue };
            let at = parse_ts(i["updatedAt"].as_str());
            let state = match i["state"].as_str() {
                Some("CLOSED") => "closed",
                _ => "open",
            };
            let labels: Vec<Value> = i
                .pointer("/labels/nodes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|l| json!({ "name": l["name"], "color": l["color"] }))
                .collect();
            acts.push(Act {
                kind: "issue",
                key: format!("{full}#{number}"),
                defaults: Value::Null,
                base: json!({
                    "number": number,
                    "title": mask(i["title"].clone()),
                    "repo": mask(json!(full)),
                    "state": state,
                    "labels": if private { json!([]) } else { Value::Array(labels) },
                    "comments": i.pointer("/comments/totalCount").and_then(Value::as_i64).unwrap_or(0),
                    "private": private,
                    "author": mask(json!(i.pointer("/author/login").and_then(Value::as_str).map(person))),
                    "url": mask(i["url"].clone()),
                    "updated_at": at,
                }),
                at,
            });
        }

        Ok(acts)
    }
}

pub fn rest_commit_to_act(full: &str, branch: Option<&str>, c: &Value) -> Option<Act> {
    let sha_full = c.get("sha").and_then(Value::as_str)?;
    let short = &sha_full[..sha_full.len().min(7)];
    let msg = c
        .pointer("/commit/message")
        .and_then(Value::as_str)
        .map(|m| m.lines().next().unwrap_or("").to_owned());
    let ts_raw = c
        .pointer("/commit/committer/date")
        .and_then(Value::as_str)
        .or_else(|| c.pointer("/commit/author/date").and_then(Value::as_str));
    let at = parse_ts(ts_raw);
    let author = c.pointer("/author/login").and_then(Value::as_str);
    let url = c.get("html_url").and_then(Value::as_str);

    Some(Act {
        kind: "commit",
        key: format!("{full}@{sha_full}"),
        defaults: Value::Null,
        base: json!({
            "sha": short,
            "message": msg,
            "repo": full,
            "branch": branch,
            "private": false,
            "author": author.map(person),
            "url": url,
            "timestamp": at,
        }),
        at,
    })
}

pub fn search_item_to_act(item: &Value) -> Option<Act> {
    let number = item.get("number").and_then(Value::as_i64)?;
    let repo = item
        .get("repository_url")
        .and_then(Value::as_str)
        .and_then(|u| u.strip_prefix("https://api.github.com/repos/"))
        .map(str::to_owned)?;
    let at = parse_ts(item.get("updated_at").and_then(Value::as_str));
    let author = item.pointer("/user/login").and_then(Value::as_str);
    let url = item.get("html_url").and_then(Value::as_str);
    let title = item.get("title").and_then(Value::as_str);

    if let Some(pr_obj) = item.get("pull_request").filter(|v| !v.is_null()) {
        let merged = pr_obj.get("merged_at").is_some_and(|v| !v.is_null());
        let state = if merged {
            "merged"
        } else {
            item.get("state").and_then(Value::as_str).unwrap_or("open")
        };
        let draft = item.get("draft").and_then(Value::as_bool).unwrap_or(false);
        return Some(Act {
            kind: "pull_request",
            key: format!("{repo}#{number}"),
            defaults: json!({
                "branch": null,
                "checks_status": null,
                "deployment_status": null,
                "review_threads": null,
                "participants": [],
            }),
            // Omit `branch` here so an existing branch from webhooks/GraphQL survives `||` merge.
            base: json!({
                "id": number,
                "number": number,
                "title": title,
                "repo": repo,
                "state": state,
                "draft": draft,
                "private": false,
                "author": author.map(person),
                "url": url,
                "updated_at": at,
            }),
            at,
        });
    }

    let labels: Vec<Value> = item
        .get("labels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|l| json!({ "name": l.get("name"), "color": l.get("color") }))
        .collect();

    Some(Act {
        kind: "issue",
        key: format!("{repo}#{number}"),
        defaults: Value::Null,
        base: json!({
            "number": number,
            "title": title,
            "repo": repo,
            "state": item.get("state").and_then(Value::as_str).unwrap_or("open"),
            "labels": Value::Array(labels),
            "comments": item.get("comments").and_then(Value::as_i64).unwrap_or(0),
            "private": false,
            "author": author.map(person),
            "url": url,
            "updated_at": at,
        }),
        at,
    })
}

fn parse_enrichment(pr: &Value) -> Enrichment {
    let nodes = pr
        .pointer("/reviewThreads/nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let resolved = nodes
        .iter()
        .filter(|n| n["isResolved"].as_bool() == Some(true))
        .count();
    let total = pr
        .pointer("/reviewThreads/totalCount")
        .and_then(Value::as_u64)
        .unwrap_or(nodes.len() as u64);

    let checks = pr
        .pointer("/commits/nodes/0/commit/statusCheckRollup/state")
        .and_then(Value::as_str)
        .map(|s| match s {
            "SUCCESS" => "success",
            "FAILURE" | "ERROR" => "failure",
            _ => "pending",
        });

    let mut seen = BTreeSet::new();
    let participants: Vec<Value> = pr
        .pointer("/reviews/nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|r| r.pointer("/author/login").and_then(Value::as_str))
        .filter(|l| seen.insert(l.to_lowercase()))
        .map(person)
        .collect();

    Enrichment {
        review_threads: json!({ "resolved": resolved, "total": total }),
        checks_status: json!(checks),
        participants: Value::Array(participants),
        merged: pr.get("merged").and_then(Value::as_bool),
    }
}

fn valid_repo_slug(repo: &str) -> bool {
    let Some((owner, name)) = repo.split_once('/') else {
        return false;
    };
    if name.contains('/') {
        return false;
    }
    [owner, name].into_iter().all(|part| {
        !part.is_empty()
            && part.len() <= 100
            && part != "."
            && part != ".."
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    })
}

fn parse_ts(raw: Option<&str>) -> DateTime<Utc> {
    raw.and_then(|x| DateTime::parse_from_rfc3339(x).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_rest_commit_and_search_items() {
        let c = json!({
            "sha": "eb8ce09abcdef1234567890",
            "html_url": "https://github.com/example-user/demo-repo/commit/eb8ce09",
            "commit": {
                "message": "Update title\n\nmore lines",
                "committer": { "date": "2026-11-08T21:19:17Z" }
            },
            "author": { "login": "example-user" }
        });
        let commit_act = rest_commit_to_act("example-user/demo-repo", Some("main"), &c).unwrap();
        assert_eq!(commit_act.kind, "commit");
        assert_eq!(commit_act.key, "example-user/demo-repo@eb8ce09abcdef1234567890");
        assert_eq!(commit_act.base["sha"], "eb8ce09");
        assert_eq!(commit_act.base["message"], "Update title");
        assert_eq!(commit_act.base["branch"], "main");
        assert_eq!(commit_act.base["private"], false);

        let issue_item = json!({
            "number": 7,
            "title": "Presence disappears after reconnect",
            "state": "closed",
            "html_url": "https://github.com/example-org/example-project/issues/7",
            "repository_url": "https://api.github.com/repos/example-org/example-project",
            "updated_at": "2026-10-26T10:26:09Z",
            "comments": 5,
            "user": { "login": "example-user" },
            "labels": [{ "name": "bug", "color": "8250df" }],
            "pull_request": null
        });
        let issue_act = search_item_to_act(&issue_item).unwrap();
        assert_eq!(issue_act.kind, "issue");
        assert_eq!(issue_act.key, "example-org/example-project#7");
        assert_eq!(issue_act.base["comments"], 5);
        assert_eq!(issue_act.base["repo"], "example-org/example-project");

        let pr_item = json!({
            "number": 42,
            "title": "Fix race",
            "state": "closed",
            "draft": false,
            "html_url": "https://github.com/example-user/demo-repo/pull/42",
            "repository_url": "https://api.github.com/repos/example-user/demo-repo",
            "updated_at": "2026-11-11T10:00:00Z",
            "user": { "login": "example-user" },
            "pull_request": { "merged_at": "2026-11-11T10:00:00Z" }
        });
        let pr_act = search_item_to_act(&pr_item).unwrap();
        assert_eq!(pr_act.kind, "pull_request");
        assert_eq!(pr_act.base["state"], "merged");
        assert!(pr_act.base.get("branch").is_none());

        assert!(valid_repo_slug("octocat/Hello-World.rs"));
        assert!(!valid_repo_slug("octocat/../secret"));
        assert!(!valid_repo_slug("octocat/a/b"));
        assert!(!valid_repo_slug("octocat/"));
    }
}
