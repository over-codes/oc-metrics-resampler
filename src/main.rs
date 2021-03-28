use chrono::prelude::*;
use log::{info, warn};
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};
use tokio::{task, time::sleep};
use tonic::{transport::Channel, Request, Status};

use proto::{
    compressed_metric::TimeValue, load_metrics_request::TimeRange, metric::Value,
    metrics_service_client::MetricsServiceClient, ListMetricsRequest, LoadMetricsRequest, Metric,
    RecordMetricsRequest,
};
//use proto::LoadMetricsReq;

pub mod proto {
    tonic::include_proto!("metrics_service");
}

const MEDIAN_PREFIX: &str = "median.5minute.";

const PREFIX: &str = "hosts.";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let remote = std::env::var("OC_METRICS_SERVER").unwrap_or_else(|_| "http://[::1]:50051".into());
    let mut client: MetricsServiceClient<Channel> = MetricsServiceClient::connect(remote).await?;

    // get a list of all metrics matching the prefix we care about
    let response = client
        .list_metrics(Request::new(ListMetricsRequest {
            prefix: PREFIX.into(),
        }))
        .await?;
    let prefixes_to_match = &response.get_ref().metrics_list;
    info!("Got {:?}", prefixes_to_match);

    // get a list of metrics which are already populated
    let response = client
        .list_metrics(Request::new(ListMetricsRequest {
            prefix: format!("{}{}", MEDIAN_PREFIX, PREFIX),
        }))
        .await?;
    // convert to a hashmap for quick lookups
    let mut last_updates: HashMap<String, Option<prost_types::Timestamp>> = HashMap::default();
    for item in &response.get_ref().metrics_list {
        last_updates.insert(
            item.identifier[MEDIAN_PREFIX.len()..].to_string(),
            item.last_timestamp.clone(),
        );
    }

    // finally, kick off a worker per identifier
    let mut threads = vec![];
    for item in prefixes_to_match {
        let identifier = item.identifier.clone();
        let last_update = last_updates.get(&identifier).unwrap_or(&None).clone();
        let client = client.clone();
        let handle = task::spawn(async move {
            if let Err(e) = spin_on_identifier(identifier.clone(), client, last_update).await {
                warn!("Resampler for {} failed with {}", identifier, e);
            }
        });
        threads.push(handle);
    }

    for h in threads {
        h.await?;
    }

    Ok(())
}

async fn spin_on_identifier(
    identifier: String,
    mut client: MetricsServiceClient<Channel>,
    mut start_time: Option<prost_types::Timestamp>,
) -> Result<(), Status> {
    info!(
        "Asked to run for {} starting at {:?}",
        identifier, start_time
    );
    // loop creating values
    loop {
        if let Some(w) = &start_time {
            let d = UNIX_EPOCH
                + Duration::from_secs(w.seconds as u64)
                + Duration::from_nanos(w.nanos as u64);
            info!("Processing time {}", DateTime::<Utc>::from(d))
        }
        let req = tonic::Request::new(LoadMetricsRequest {
            prefix: identifier.clone(),
            max_time_values: 1000,
            time_range: Some(TimeRange {
                start: start_time.clone(),
                stop: None,
            }),
            value_type: 0,
        });
        let response = client.load_metrics(req).await?;
        let response = response.get_ref();
        for metric in &response.metrics {
            let (next_time, vals) = median_metrics(&metric.identifier, &metric.time_values);
            info!("Writing {} metrics", vals.len());
            if !vals.is_empty() {
                start_time = Some(next_time);
                let req = tonic::Request::new(RecordMetricsRequest {
                    metrics: vals.clone(),
                });
                match client.record_metrics(req).await {
                    Ok(_) => (),
                    Err(e) => {
                        warn!("Error from server: {}; ignoring as this is likely transitive; was writing {:#?}", e, vals)
                    }
                };
            } else {
                sleep(Duration::from_secs(60)).await;
            }
        }
    }
}

fn median_metrics(identifier: &str, tv: &[TimeValue]) -> (Timestamp, Vec<Metric>) {
    if tv.is_empty() {
        return (
            Timestamp {
                seconds: 0,
                nanos: 0,
            },
            vec![],
        );
    }
    // get the base time, rounded to the nearest 5 minute window
    let mut start = align_to_5_minute(tv[0].when.as_ref().unwrap().seconds);
    let mut ret = vec![];
    let mut vals_so_far = vec![];
    for m in tv {
        if m.when.as_ref().unwrap().seconds > start + 60 * 5 {
            if vals_so_far.is_empty() {
                ret.push(Metric {
                    identifier: format!("{}{}", MEDIAN_PREFIX, identifier),
                    value: Some(Value::DoubleValue(median(&mut vals_so_far))),
                    when: Some(Timestamp {
                        seconds: start,
                        nanos: 0,
                    }),
                });
                vals_so_far = vec![];
            }
            start = align_to_5_minute(m.when.as_ref().unwrap().seconds);
        }
        if let Some(proto::compressed_metric::time_value::Value::DoubleValue(v)) = m.value {
            vals_so_far.push(v);
        }
    }
    (
        Timestamp {
            seconds: start,
            nanos: 0,
        },
        ret,
    )
}

fn align_to_5_minute(seconds: i64) -> i64 {
    let min5 = 5 * 60;
    seconds - (seconds % min5)
}

fn median(vals: &mut Vec<f64>) -> f64 {
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if vals.len() % 2 == 0 {
        (vals[vals.len() / 2 - 1] + vals[vals.len() / 2]) / 2.0
    } else {
        vals[vals.len() / 2]
    }
}
