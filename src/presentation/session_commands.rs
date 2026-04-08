use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::domain::entities::Session;
use crate::infrastructure::networking::resp::RespEncoder;
use crate::presentation::command::router::CommandRouter;

pub(super) fn handle_auth(args: &[String], session: &mut Session, router: &CommandRouter) -> Bytes {
    let (username, password) = match args.len() {
        2 => ("default", args[1].as_str()),
        3 => (args[1].as_str(), args[2].as_str()),
        _ => return RespEncoder::error("wrong number of arguments for 'auth' command"),
    };
    if router.authenticate(username, password) {
        session.authenticate(username.to_string());
        RespEncoder::simple_string("OK")
    } else {
        RespEncoder::raw_error("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

pub(super) fn handle_watch(args: &[String], session: &mut Session, router: &CommandRouter) -> Bytes {
    if session.is_tx_active() {
        return RespEncoder::error("WATCH inside MULTI is not allowed");
    }
    if args.len() < 2 {
        return RespEncoder::error("wrong number of arguments for 'watch' command");
    }
    for key in args.iter().skip(1) {
        session.watch(key, router.key_version(key));
    }
    RespEncoder::simple_string("OK")
}

pub(super) fn handle_exec(session: &mut Session, router: &CommandRouter) -> Bytes {
    if !session.is_tx_active() {
        return RespEncoder::error("EXEC without MULTI");
    }
    let watched: Vec<(String, u64)> = session.watched_versions()
        .iter().map(|(k, v)| (k.clone(), *v)).collect();
    let queue = session.execute_tx();
    session.unwatch();
    router.exec_transaction(&watched, queue)
}

pub(super) fn handle_subscribe(
    args: &[String], session: &mut Session,
    router: &CommandRouter, tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
        if !session.is_subscribed_to(channel) { router.subscribe(channel, tx.clone()); }
        let count = session.subscribe(channel);
        out.extend_from_slice(&RespEncoder::array(vec![
            RespEncoder::bulk_string("subscribe"),
            RespEncoder::bulk_string(channel),
            RespEncoder::integer(count),
        ]));
        out
    }).freeze()
}

pub(super) fn handle_unsubscribe(
    args: &[String], session: &mut Session,
    router: &CommandRouter, tx: &mpsc::UnboundedSender<Bytes>,
) -> Bytes {
    args.iter().skip(1).fold(BytesMut::new(), |mut out, channel| {
        router.unsubscribe(channel, tx);
        let count = session.unsubscribe(channel);
        out.extend_from_slice(&RespEncoder::array(vec![
            RespEncoder::bulk_string("unsubscribe"),
            RespEncoder::bulk_string(channel),
            RespEncoder::integer(count),
        ]));
        out
    }).freeze()
}
