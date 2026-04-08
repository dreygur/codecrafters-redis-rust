use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::application::use_cases::stream_commands::encode_xread_results;
use crate::infrastructure::networking::resp::RespEncoder;
use crate::presentation::command::router::CommandRouter;

pub(super) async fn handle_blpop(
    args: &[String],
    router: &CommandRouter,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<()> {
    if args.len() < 3 {
        writer.write_all(&RespEncoder::error("wrong number of arguments for 'blpop' command")).await?;
        return Ok(());
    }

    let key = &args[1];
    let timeout_secs: f64 = args.last().and_then(|s| s.parse().ok()).unwrap_or(0.0);

    match router.blpop_or_wait(key) {
        Ok(value) => {
            writer.write_all(&blpop_response(key, &value)).await?;
        }
        Err(receiver) => {
            let result = if timeout_secs == 0.0 {
                receiver.await.ok()
            } else {
                let duration = std::time::Duration::from_secs_f64(timeout_secs);
                tokio::time::timeout(duration, receiver).await.ok().and_then(|r| r.ok())
            };

            match result {
                Some(value) => writer.write_all(&blpop_response(key, &value)).await?,
                None => writer.write_all(&RespEncoder::null_array()).await?,
            }
        }
    }

    Ok(())
}

fn blpop_response(key: &str, value: &str) -> Bytes {
    RespEncoder::array(vec![
        RespEncoder::bulk_string(key),
        RespEncoder::bulk_string(value),
    ])
}

pub(super) async fn handle_xread_block(
    args: &[String],
    router: &CommandRouter,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<()> {
    let Some(parsed) = parse_xread_args(args) else {
        writer.write_all(&RespEncoder::error("syntax error")).await?;
        return Ok(());
    };

    let block_ms = parsed.block_ms.unwrap_or(0);

    // Resolve "$" to the stream's current last entry ID at the time of the command
    let resolved_ids: Vec<String> = parsed.keys.iter().zip(parsed.ids.iter())
        .map(|(key, id)| {
            if id == "$" {
                let (ms, seq) = router.stream_last_id(key);
                format!("{}-{}", ms, seq)
            } else {
                id.clone()
            }
        })
        .collect();

    // Check all streams non-blocking first
    let immediate = router.xread_non_blocking(&parsed.keys, &resolved_ids);
    if !immediate.is_empty() {
        writer.write_all(&encode_xread_results(immediate, parsed.count)).await?;
        return Ok(());
    }

    // Register a shared channel across all watched keys, then block
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String, Vec<(String, String)>)>();

    for (key, id_str) in parsed.keys.iter().zip(resolved_ids.iter()) {
        let after_id = parse_id_pair(id_str);
        // Atomic: if entries exist now (race between check above and register), return them
        if let Some(entries) = router.xread_blocking(key, after_id, tx.clone()) {
            let results = vec![(key.clone(), entries)];
            writer.write_all(&encode_xread_results(results, parsed.count)).await?;
            return Ok(());
        }
    }

    let notification = if block_ms == 0 {
        rx.recv().await
    } else {
        let duration = std::time::Duration::from_millis(block_ms);
        tokio::time::timeout(duration, rx.recv()).await.ok().flatten()
    };

    match notification {
        Some((key, entry_id, fields)) => {
            let results = vec![(key, vec![(entry_id, fields)])];
            writer.write_all(&encode_xread_results(results, parsed.count)).await?;
        }
        None => {
            writer.write_all(&RespEncoder::null_array()).await?;
        }
    }

    Ok(())
}

struct XReadArgs {
    count: Option<usize>,
    block_ms: Option<u64>,
    keys: Vec<String>,
    ids: Vec<String>,
}

fn parse_xread_args(args: &[String]) -> Option<XReadArgs> {
    let mut count = None;
    let mut block_ms = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "COUNT" => {
                count = args.get(i + 1)?.parse().ok();
                i += 2;
            }
            "BLOCK" => {
                block_ms = Some(args.get(i + 1)?.parse::<u64>().ok()?);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return None,
        }
    }

    let remaining = &args[i..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return None;
    }
    let mid = remaining.len() / 2;

    Some(XReadArgs {
        count,
        block_ms,
        keys: remaining[..mid].to_vec(),
        ids: remaining[mid..].to_vec(),
    })
}

fn parse_id_pair(id: &str) -> (u64, u64) {
    id.split_once('-')
        .and_then(|(ms, seq)| Some((ms.parse().ok()?, seq.parse().ok()?)))
        .unwrap_or((0, 0))
}
