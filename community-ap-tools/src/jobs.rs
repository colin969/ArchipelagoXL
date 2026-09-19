use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use anyhow::Result;
use deadpool_redis::redis;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool;
use rocket::tokio;
use semver::Version;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;
use wq::{JobDesc, JobResult, JobStatus, WorkQueue};

use crate::review::db;

#[derive(Serialize, Deserialize)]
pub struct YamlAnalysisParams {
    pub room_id: String,
    pub yaml_id: String,
    pub apworlds: Vec<(String, Version)>,
    pub otlp_context: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct YamlAnalysisResponse {
    pub total_checks: Option<u32>,
    pub starting_checks: Option<u32>,
    pub status: Option<String>,
    pub gen_ms: Option<u32>,
    pub error: Option<String>,
}

pub type YamlAnalysisQueue = WorkQueue<YamlAnalysisParams, YamlAnalysisResponse>;

const INFLIGHT_TTL_SECS: u64 = 3600; // 1 hour — safely longer than max analysis time

fn inflight_key(yaml_id: Uuid) -> String {
    format!("yaml_analysis:inflight:{}", yaml_id)
}

async fn release_inflight_key_conn(redis: &mut deadpool_redis::Connection, key: &str) {
    let _: Result<(), _> = redis::cmd("DEL").arg(key).query_async(&mut **redis).await;
}

async fn release_inflight_key(redis_pool: &deadpool_redis::Pool, key: &str) {
    match redis_pool.get().await {
        Ok(mut redis) => release_inflight_key_conn(&mut redis, key).await,
        Err(e) => tracing::error!(
            "Failed to get Redis connection to release inflight key {}: {}",
            key,
            e
        ),
    }
}

pub fn get_yaml_analysis_callback(
    db_pool: Pool<AsyncPgConnection>,
    redis_pool: deadpool_redis::Pool,
) -> wq::ResolveCallback<YamlAnalysisParams, YamlAnalysisResponse> {
    let callback = move |desc: JobDesc<YamlAnalysisParams>,
                         result: JobResult<YamlAnalysisResponse>|
          -> Pin<Box<dyn Future<Output = Result<bool>> + Send>> {
        let inner_pool = db_pool.clone();
        let inner_redis = redis_pool.clone();

        let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&desc.params.otlp_context)
        });
        let span = tracing::info_span!(
            "yaml_analysis_callback",
            yaml_id = %desc.params.yaml_id,
            job_status = ?result.status,
        );
        let _ = span.set_parent(parent_cx);

        Box::pin(
            async move {
                let Ok(yaml_id) = desc.params.yaml_id.parse::<Uuid>() else {
                    tracing::warn!("yaml_analysis_callback: invalid yaml_id, skipping");
                    return Ok(true);
                };
                let Ok(room_id) = desc.params.room_id.parse::<Uuid>() else {
                    tracing::warn!("yaml_analysis_callback: invalid room_id, skipping");
                    return Ok(true);
                };

                let key = inflight_key(yaml_id);

                let mut conn = match inner_pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("Failed to get DB connection in callback: {}", e);
                        release_inflight_key(&inner_redis, &key).await;
                        return Err(e.into());
                    }
                };

                match result.status {
                    JobStatus::Success => {
                        let Some(response) = result.result else {
                            tracing::warn!("yaml_analysis job succeeded but returned no result");
                            return Ok(true);
                        };

                        let status = response.status.as_deref().unwrap_or("success");
                        let success = if response.error.as_ref().map(|e| !e.is_empty()).unwrap_or(false) { 0 } else { 1 };

                        if let Err(e) = db::set_yaml_analysis_status(
                            room_id,
                            yaml_id,
                            status,
                            success,
                            response.total_checks.unwrap_or(0) as i32,
                            response.starting_checks.unwrap_or(0) as i32,
                            response.gen_ms.unwrap_or(0) as i32,
                            &mut conn,
                        )
                        .await
                        {
                            tracing::error!("Failed to persist yaml analysis result, releasing inflight key: {}", e);
                            release_inflight_key(&inner_redis, &key).await;
                            return Err(e.into());
                        }
                    }
                    
                    JobStatus::Failure => {
                        let error = result
                            .result
                            .as_ref()                                       // borrow instead of consume
                            .and_then(|r| r.error.as_deref())
                            .unwrap_or("Unknown failure");
                        tracing::warn!("yaml_analysis job failed: {}", error);

                        let status = result.result                          // now safe to consume
                            .and_then(|r| r.status)
                            .unwrap_or_else(|| "generic failure".to_string());
                        
                        db::set_yaml_analysis_status(room_id, yaml_id, &status, 0, 0, 0, 0, &mut conn).await?;
                    }
                    JobStatus::InternalError => {
                        tracing::error!("yaml_analysis job encountered an internal error");

                        db::set_yaml_analysis_status(room_id, yaml_id, "internal error", 0, 0, 0, 0, &mut conn).await?;
                    }
                    _ => {
                        tracing::warn!(
                            "yaml_analysis_callback: unhandled job status {:?}, skipping",
                            result.status
                        );
                    }
                }

                // Inflight key expires naturally via TTL — no explicit cleanup needed.
                // Removing it here just allows faster re-queueing if the yaml is somehow
                // re-submitted before TTL expires (e.g. manual re-analysis trigger).
                Ok(true)
            }
            .instrument(span),
        )
    };

    Arc::pin(callback)
}

pub async fn priority_queue_yaml(
    room_id: Uuid,
    yaml_id: Uuid,
    queue: &YamlAnalysisQueue,
    db_pool: &Pool<AsyncPgConnection>,
    redis_pool: &deadpool_redis::Pool,
) -> Result<()> {
    let mut conn = db_pool.get().await.map_err(anyhow::Error::from)?;

    // Delete existing analysis result
    db::reset_yaml_analysis_status(room_id, yaml_id, &mut conn).await?;

    // Clear the inflight key so the job can be re-queued immediately
    let key = inflight_key(yaml_id);
    release_inflight_key(redis_pool, &key).await;

    let apworlds_json = db::get_yaml_apworlds(yaml_id, &mut conn).await?;
    let raw: Vec<serde_json::Value> = serde_json::from_str(&apworlds_json)?;
    let apworlds: Vec<(String, Version)> = raw
        .into_iter()
        .filter_map(|v| {
            let name = v["name"].as_str()?.to_string();
            let ver = v["version"].as_str()?;
            Version::parse(ver).ok().map(|v| (name, v))
        })
        .collect();

    // Acquire the inflight key
    let mut redis = redis_pool.get().await.map_err(anyhow::Error::from)?;
    let acquired: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg(1)
        .arg("NX")
        .arg("EX")
        .arg(INFLIGHT_TTL_SECS)
        .query_async(&mut *redis)
        .await?;

    // Cover race con
    if acquired.is_none() {
        tracing::info!("force_reanalyze_yaml: inflight key already set for {}, skipping", yaml_id);
        return Ok(());
    }

    let params = YamlAnalysisParams {
        room_id: room_id.to_string(),
        yaml_id: yaml_id.to_string(),
        apworlds,
        otlp_context: HashMap::new(),
    };

    match queue
        .enqueue_job(&params, wq::Priority::High, Duration::from_secs(1800))
        .await
    {
        Ok(job_id) => {
            eprintln!("[YAML_ANALYSIS_QUEUE] Priority re-queued yaml {} as job {}", yaml_id, job_id);
        }
        Err(e) => {
            eprintln!("[YAML_ANALYSIS_QUEUE] Failed to priority re-queue yaml {}: {}", yaml_id, e);
            release_inflight_key_conn(&mut redis, &key).await;
            return Err(e.into());
        }
    }

    tracing::info!("force_reanalyze_yaml: queued re-analysis for yaml {}", yaml_id);
    Ok(())
}


pub fn start_unanalyzed_yaml_poller(
    db_pool: Pool<AsyncPgConnection>,
    redis_pool: deadpool_redis::Pool,
    queue: YamlAnalysisQueue,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));

        loop {
            interval.tick().await;

            let mut redis = match redis_pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "[YAML_ANALYSIS_QUEUE] Failed to get Redis connection: {}",
                        e
                    );
                    continue;
                }
            };

            let mut conn = match db_pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[YAML_ANALYSIS_QUEUE] Failed to get DB connection: {}", e);
                    continue;
                }
            };

            let rows: Vec<db::UnanalyzedYamlRow> = match db::get_unanalyzed_yamls(&mut conn).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "[YAML_ANALYSIS_QUEUE] Failed to fetch unanalyzed yamls: {}",
                        e
                    );
                    continue;
                }
            };

            for row in rows {
                let key = inflight_key(row.yaml_id);

                let acquired: Option<String> = match redis::cmd("SET")
                    .arg(&key)
                    .arg(1)
                    .arg("NX")
                    .arg("EX")
                    .arg(INFLIGHT_TTL_SECS)
                    .query_async(&mut *redis)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!(
                            "[YAML_ANALYSIS_QUEUE] Redis SET NX failed for {}: {}",
                            row.yaml_id, e
                        );
                        continue;
                    }
                };

                if acquired.is_none() {
                    // Key already exists — job is in-flight
                    continue;
                }

                let apworlds: Vec<(String, Version)> =
                    match serde_json::from_str::<Vec<serde_json::Value>>(&row.apworlds_json) {
                        Ok(raw) => raw
                            .into_iter()
                            .filter_map(|v| {
                                let name = v["name"].as_str()?.to_string();
                                let ver = v["version"].as_str()?;
                                Version::parse(ver).ok().map(|v| (name, v))
                            })
                            .collect(),
                        Err(e) => {
                            eprintln!(
                                "[YAML_ANALYSIS_QUEUE] Failed to parse apworlds for yaml {}: {}",
                                row.yaml_id, e
                            );
                            // Release the inflight key so it can be retried next poll
                            release_inflight_key_conn(&mut redis, &key).await;
                            continue;
                        }
                    };

                let params = YamlAnalysisParams {
                    room_id: row.room_id.to_string(),
                    yaml_id: row.yaml_id.to_string(),
                    apworlds,
                    otlp_context: HashMap::new(),
                };

                match queue
                    .enqueue_job(&params, wq::Priority::Normal, Duration::from_secs(1800))
                    .await
                {
                    Ok(job_id) => {
                        eprintln!(
                            "[YAML_ANALYSIS_QUEUE] Submitted yaml {} as job {}",
                            row.yaml_id, job_id
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "[YAML_ANALYSIS_QUEUE] Failed to submit yaml {}: {}",
                            row.yaml_id, e
                        );
                        release_inflight_key_conn(&mut redis, &key).await;
                    }
                }
            }
        }
    });
}
