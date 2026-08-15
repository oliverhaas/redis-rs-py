// PubSub façade — sync `PubSub` and async `AsyncPubSub` pyclasses with
// dedicated-connection bridge into Python via tokio mpsc + RedisRsAwaitable.
//
// See plan 14 for the full architecture. The supervisor task owns the
// physical connection's lifetime:
//   - Drains redis::aio::PubSub::on_message() into the bridge outbound channel.
//   - Handles subscribe/unsubscribe commands from the pyclass.
//   - Reconnects with backoff when the stream ends unexpectedly.
//   - Sends periodic PING health-checks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyTuple};
use redis::aio::{PubSub as RedisPubSub, PubSubSink, PubSubStream};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::time;

use crate::async_bridge::{RawResult, RedisRsAwaitable};
use crate::exceptions::{DataError, PubSubError};
use crate::runtime::get_runtime;

// =========================================================================
// Message types
// =========================================================================

/// One pubsub message bound for Python. Mirrors redis-py's dict shape.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub kind: PubSubMessageKind,
    /// Pattern matched (only set for `pmessage`).
    pub pattern: Option<Vec<u8>>,
    /// Channel name. For confirmation messages, the channel/pattern that
    /// was (un)subscribed.
    pub channel: Vec<u8>,
    /// Payload. For real messages it's bytes; for confirmation messages
    /// it's the subscriber count.
    pub data: PubSubData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PubSubMessageKind {
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
    SSubscribe,
    SUnsubscribe,
    Message,
    PMessage,
    SMessage,
    /// Health-check pong — internal; suppressed before reaching Python.
    Pong,
}

impl PubSubMessageKind {
    fn type_str(self) -> &'static str {
        match self {
            PubSubMessageKind::Subscribe => "subscribe",
            PubSubMessageKind::Unsubscribe => "unsubscribe",
            PubSubMessageKind::PSubscribe => "psubscribe",
            PubSubMessageKind::PUnsubscribe => "punsubscribe",
            PubSubMessageKind::SSubscribe => "ssubscribe",
            PubSubMessageKind::SUnsubscribe => "sunsubscribe",
            PubSubMessageKind::Message => "message",
            PubSubMessageKind::PMessage => "pmessage",
            PubSubMessageKind::SMessage => "smessage",
            PubSubMessageKind::Pong => "pong",
        }
    }

    pub fn is_subscribe_confirmation(self) -> bool {
        matches!(
            self,
            PubSubMessageKind::Subscribe
                | PubSubMessageKind::Unsubscribe
                | PubSubMessageKind::PSubscribe
                | PubSubMessageKind::PUnsubscribe
                | PubSubMessageKind::SSubscribe
                | PubSubMessageKind::SUnsubscribe
        )
    }
}

#[derive(Debug, Clone)]
pub enum PubSubData {
    Bytes(Vec<u8>),
    Count(i64),
}

impl PubSubMessage {
    /// Convert the message into a redis-py-shaped dict.
    pub fn into_py_dict(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("type", self.kind.type_str())?;
        match &self.pattern {
            Some(p) => dict.set_item("pattern", PyBytes::new(py, p))?,
            None => dict.set_item("pattern", py.None())?,
        }
        dict.set_item("channel", PyBytes::new(py, &self.channel))?;
        match self.data {
            PubSubData::Bytes(ref b) => dict.set_item("data", PyBytes::new(py, b))?,
            PubSubData::Count(n) => dict.set_item("data", n)?,
        }
        Ok(dict.into_any().unbind())
    }
}

// =========================================================================
// Subscription state
// =========================================================================

/// Subscription state, tracked so the reconnect path can replay it.
#[derive(Default, Debug, Clone)]
pub struct SubscriptionState {
    pub channels: Vec<Vec<u8>>,
    pub patterns: Vec<Vec<u8>>,
    pub shard_channels: Vec<Vec<u8>>,
}

// =========================================================================
// Bridge types
// =========================================================================

/// Commands the pyclass sends into the bridge.
#[derive(Debug)]
pub enum BridgeCommand {
    Subscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    Unsubscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    PSubscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    PUnsubscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    SSubscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    SUnsubscribe(
        Vec<Vec<u8>>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    Shutdown,
}

/// The handle that lives on the pyclass.
pub struct PubSubBridge {
    /// Receiver end of the user-visible message stream.
    pub outbound: AsyncMutex<mpsc::UnboundedReceiver<PubSubMessage>>,
    /// Sender into the command channel that the supervisor drains.
    pub commands: mpsc::UnboundedSender<BridgeCommand>,
    /// Shared subscription state — read by reconnect, mutated under each
    /// subscribe/unsubscribe call.
    pub subs: Arc<std::sync::Mutex<SubscriptionState>>,
    /// Set to true when close() is called so blocked recv can detect it.
    pub closed: std::sync::atomic::AtomicBool,
}

impl Drop for PubSubBridge {
    fn drop(&mut self) {
        let _ = self.commands.send(BridgeCommand::Shutdown);
    }
}

impl PubSubBridge {
    /// Build a bridge from a redis::Client. Opens a dedicated pubsub
    /// connection, spawns the supervisor task, and returns the bridge.
    pub async fn spawn(
        client: redis::Client,
        health_check_interval: Duration,
    ) -> Result<Arc<PubSubBridge>, String> {
        let pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|e| format!("get_async_pubsub failed: {e}"))?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<PubSubMessage>();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<BridgeCommand>();
        let subs = Arc::new(std::sync::Mutex::new(SubscriptionState::default()));

        let bridge = Arc::new(PubSubBridge {
            outbound: AsyncMutex::new(out_rx),
            commands: cmd_tx,
            subs: subs.clone(),
            closed: std::sync::atomic::AtomicBool::new(false),
        });

        get_runtime().spawn(supervisor_task(
            pubsub,
            client,
            cmd_rx,
            out_tx,
            subs,
            health_check_interval,
        ));

        Ok(bridge)
    }
}

// =========================================================================
// Supervisor task
// =========================================================================

async fn supervisor_task(
    pubsub: RedisPubSub,
    client: redis::Client,
    mut cmd_rx: mpsc::UnboundedReceiver<BridgeCommand>,
    out_tx: mpsc::UnboundedSender<PubSubMessage>,
    subs: Arc<std::sync::Mutex<SubscriptionState>>,
    health_check_interval: Duration,
) {
    // Split the pubsub into independent sink (for commands) and stream (for messages).
    // This lets us select! on them without borrow conflicts.
    let (mut sink, mut stream) = pubsub.split();
    let mut next_ping = time::Instant::now() + health_check_interval;

    loop {
        let now = time::Instant::now();
        let sleep_for = if next_ping > now {
            next_ping - now
        } else {
            Duration::from_millis(1)
        };

        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(BridgeCommand::Shutdown) | None => return,
                    Some(other) => {
                        if let Err(true) =
                            handle_command(&mut sink, other, &subs, &out_tx).await
                        {
                            if let Some((new_sink, new_stream)) =
                                try_reconnect(&client, &subs).await
                            {
                                sink = new_sink;
                                stream = new_stream;
                            } else {
                                return;
                            }
                        }
                    }
                }
            }
            maybe_msg = stream.next() => {
                match maybe_msg {
                    Some(msg) => forward_message(msg, &out_tx),
                    None => {
                        // Stream ended — connection died. Try to reconnect.
                        if let Some((new_sink, new_stream)) =
                            try_reconnect(&client, &subs).await
                        {
                            sink = new_sink;
                            stream = new_stream;
                        } else {
                            return;
                        }
                    }
                }
            }
            _ = time::sleep(sleep_for) => {
                next_ping = time::Instant::now() + health_check_interval;
                let _: redis::RedisResult<redis::Value> = sink.ping().await;
            }
        }
    }
}

async fn handle_command(
    sink: &mut PubSubSink,
    cmd: BridgeCommand,
    subs: &Arc<std::sync::Mutex<SubscriptionState>>,
    out_tx: &mpsc::UnboundedSender<PubSubMessage>,
) -> Result<(), bool> {
    let (op_result, kind, names) = match cmd {
        BridgeCommand::Subscribe(names, ack) => {
            let r = sink.subscribe(names.as_slice()).await;
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::Subscribe, names)
        }
        BridgeCommand::Unsubscribe(names, ack) => {
            let r = if names.is_empty() {
                sink.unsubscribe(&[] as &[Vec<u8>]).await
            } else {
                sink.unsubscribe(names.as_slice()).await
            };
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::Unsubscribe, names)
        }
        BridgeCommand::PSubscribe(names, ack) => {
            let r = sink.psubscribe(names.as_slice()).await;
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::PSubscribe, names)
        }
        BridgeCommand::PUnsubscribe(names, ack) => {
            let r = if names.is_empty() {
                sink.punsubscribe(&[] as &[Vec<u8>]).await
            } else {
                sink.punsubscribe(names.as_slice()).await
            };
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::PUnsubscribe, names)
        }
        BridgeCommand::SSubscribe(names, ack) => {
            // redis-rs 1.2 doesn't expose ssubscribe directly on PubSub.
            let r: redis::RedisResult<()> = Err(redis::RedisError::from((
                redis::ErrorKind::Client,
                "ssubscribe not yet supported in redis-rs 1.2 PubSub",
            )));
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::SSubscribe, names)
        }
        BridgeCommand::SUnsubscribe(names, ack) => {
            let r: redis::RedisResult<()> = Err(redis::RedisError::from((
                redis::ErrorKind::Client,
                "sunsubscribe not yet supported in redis-rs 1.2 PubSub",
            )));
            let _ = ack.send(r.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            (r, PubSubMessageKind::SUnsubscribe, names)
        }
        BridgeCommand::Shutdown => unreachable!("filtered above"),
    };

    let was_io_error = match &op_result {
        Err(e) => matches!(e.kind(), redis::ErrorKind::Io),
        Ok(()) => false,
    };

    if op_result.is_ok() {
        // Update shared subscription state and fabricate a confirmation
        // message so Python sees one (redis-rs swallows them internally).
        update_subscription_state(subs, kind, &names);
        emit_subscribe_confirmations(out_tx, kind, &names, subscriber_count(subs));
    }

    if was_io_error { Err(true) } else { Ok(()) }
}

fn update_subscription_state(
    subs: &Arc<std::sync::Mutex<SubscriptionState>>,
    kind: PubSubMessageKind,
    names: &[Vec<u8>],
) {
    let mut g = subs.lock().unwrap();
    match kind {
        PubSubMessageKind::Subscribe => {
            for n in names {
                if !g.channels.iter().any(|c| c == n) {
                    g.channels.push(n.clone());
                }
            }
        }
        PubSubMessageKind::Unsubscribe => {
            if names.is_empty() {
                g.channels.clear();
            } else {
                g.channels.retain(|c| !names.iter().any(|n| n == c));
            }
        }
        PubSubMessageKind::PSubscribe => {
            for n in names {
                if !g.patterns.iter().any(|p| p == n) {
                    g.patterns.push(n.clone());
                }
            }
        }
        PubSubMessageKind::PUnsubscribe => {
            if names.is_empty() {
                g.patterns.clear();
            } else {
                g.patterns.retain(|p| !names.iter().any(|n| n == p));
            }
        }
        PubSubMessageKind::SSubscribe => {
            for n in names {
                if !g.shard_channels.iter().any(|c| c == n) {
                    g.shard_channels.push(n.clone());
                }
            }
        }
        PubSubMessageKind::SUnsubscribe => {
            if names.is_empty() {
                g.shard_channels.clear();
            } else {
                g.shard_channels.retain(|c| !names.iter().any(|n| n == c));
            }
        }
        _ => {}
    }
}

fn subscriber_count(subs: &Arc<std::sync::Mutex<SubscriptionState>>) -> i64 {
    let g = subs.lock().unwrap();
    (g.channels.len() + g.patterns.len() + g.shard_channels.len()) as i64
}

fn emit_subscribe_confirmations(
    out_tx: &mpsc::UnboundedSender<PubSubMessage>,
    kind: PubSubMessageKind,
    names: &[Vec<u8>],
    count: i64,
) {
    if names.is_empty() {
        let _ = out_tx.send(PubSubMessage {
            kind,
            pattern: None,
            channel: Vec::new(),
            data: PubSubData::Count(count),
        });
        return;
    }
    for n in names {
        let _ = out_tx.send(PubSubMessage {
            kind,
            pattern: None,
            channel: n.clone(),
            data: PubSubData::Count(count),
        });
    }
}

fn forward_message(msg: redis::Msg, out_tx: &mpsc::UnboundedSender<PubSubMessage>) {
    let channel = msg.get_channel_name().as_bytes().to_vec();
    let payload = msg.get_payload_bytes().to_vec();
    let is_pattern = msg.from_pattern();
    let kind = if is_pattern {
        PubSubMessageKind::PMessage
    } else {
        PubSubMessageKind::Message
    };
    let pattern = if is_pattern {
        msg.get_pattern::<Vec<u8>>().ok()
    } else {
        None
    };
    let _ = out_tx.send(PubSubMessage {
        kind,
        pattern,
        channel,
        data: PubSubData::Bytes(payload),
    });
}

async fn try_reconnect(
    client: &redis::Client,
    subs: &Arc<std::sync::Mutex<SubscriptionState>>,
) -> Option<(PubSubSink, PubSubStream)> {
    for attempt in 0u32..32 {
        let backoff = Duration::from_millis((50u64 * (1u64 << attempt.min(4))).min(1000));
        time::sleep(backoff).await;
        match client.get_async_pubsub().await {
            Ok(new_ps) => {
                let snapshot = {
                    let g = subs.lock().unwrap();
                    g.clone()
                };
                let (mut new_sink, new_stream) = new_ps.split();
                for ch in &snapshot.channels {
                    let _ = new_sink.subscribe(ch.as_slice()).await;
                }
                for pat in &snapshot.patterns {
                    let _ = new_sink.psubscribe(pat.as_slice()).await;
                }
                return Some((new_sink, new_stream));
            }
            Err(_) => continue,
        }
    }
    None
}

// =========================================================================
// Sync PubSub pyclass
// =========================================================================

#[pyclass(module = "redis_rs_py._driver", name = "PubSub")]
pub struct PubSub {
    pub(crate) bridge: Option<Arc<PubSubBridge>>,
    pub(crate) channel_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) pattern_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) shard_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) ignore_subscribe_messages: bool,
    #[allow(dead_code)]
    pub(crate) health_check_interval: Duration,
}

// =========================================================================
// Async PubSub pyclass
// =========================================================================

#[pyclass(module = "redis_rs_py._driver.asyncio", name = "PubSub")]
pub struct AsyncPubSub {
    pub(crate) bridge: Option<Arc<PubSubBridge>>,
    pub(crate) channel_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) pattern_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) shard_handlers: std::sync::Mutex<HashMap<Vec<u8>, Py<PyAny>>>,
    pub(crate) ignore_subscribe_messages: bool,
    #[allow(dead_code)]
    pub(crate) health_check_interval: Duration,
}

// =========================================================================
// PubSubWorkerThread pyclass
// =========================================================================

#[pyclass(module = "redis_rs_py._driver", name = "PubSubWorkerThread")]
pub struct PubSubWorkerThread {
    pub(crate) thread: std::sync::Mutex<Option<Py<PyAny>>>,
    pub(crate) running: Arc<std::sync::atomic::AtomicBool>,
}

// =========================================================================
// Shared helpers
// =========================================================================

fn parse_timeout(py: Python<'_>, t: &Py<PyAny>) -> PyResult<Option<Duration>> {
    if t.is_none(py) {
        return Ok(None);
    }
    let secs: f64 = t.extract(py)?;
    if secs <= 0.0 {
        // redis-py treats 0 as "non-blocking poll" — model as 1ms wait.
        return Ok(Some(Duration::from_millis(1)));
    }
    Ok(Some(Duration::from_secs_f64(secs)))
}

fn coerce_name(v: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = v.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(s) = v.extract::<&str>() {
        return Ok(s.as_bytes().to_vec());
    }
    Err(DataError::new_err(
        "channel/pattern names must be str or bytes",
    ))
}

fn send_command<F>(ps: &PubSub, py: Python<'_>, build: F) -> PyResult<()>
where
    F: FnOnce(&PubSubBridge, tokio::sync::oneshot::Sender<Result<(), String>>) -> BridgeCommand,
{
    let bridge = ps
        .bridge
        .as_ref()
        .ok_or_else(|| PubSubError::new_err("pubsub is closed"))?;
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let cmd = build(bridge, ack_tx);
    bridge
        .commands
        .send(cmd)
        .map_err(|_| PubSubError::new_err("pubsub bridge has shut down"))?;
    let result = py.detach(|| {
        get_runtime()
            .block_on(async move { ack_rx.await.map_err(|_| "ack channel dropped".to_string()) })
    });
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(PubSubError::new_err(e)),
        Err(e) => Err(PubSubError::new_err(e)),
    }
}

fn async_send_command<F>(ps: &AsyncPubSub, py: Python<'_>, build: F) -> PyResult<Py<PyAny>>
where
    F: FnOnce(&PubSubBridge, tokio::sync::oneshot::Sender<Result<(), String>>) -> BridgeCommand,
{
    let bridge = ps
        .bridge
        .as_ref()
        .ok_or_else(|| PubSubError::new_err("pubsub is closed"))?;
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let cmd = build(bridge, ack_tx);
    bridge
        .commands
        .send(cmd)
        .map_err(|_| PubSubError::new_err("pubsub bridge has shut down"))?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let awaitable = RedisRsAwaitable::new(rx);
    get_runtime().spawn(async move {
        let result = match ack_rx.await {
            Ok(Ok(())) => RawResult::Nil,
            Ok(Err(e)) => RawResult::Error(crate::exceptions::ExceptionClass::PubSubError, e),
            Err(_) => RawResult::Error(
                crate::exceptions::ExceptionClass::PubSubError,
                "ack channel dropped".to_string(),
            ),
        };
        let _ = tx.send(result);
    });
    Ok(awaitable.into_pyobject(py)?.into_any().unbind())
}

// =========================================================================
// #[pymethods] impl PubSub
// =========================================================================

#[pymethods]
impl PubSub {
    #[pyo3(signature = (*args, **kwargs))]
    fn subscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.channel_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "subscribe() requires at least one channel",
            ));
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::Subscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn unsubscribe(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.channel_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.channel_handlers.lock().unwrap().clear();
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::Unsubscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn psubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.pattern_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "psubscribe() requires at least one pattern",
            ));
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::PSubscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn punsubscribe(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.pattern_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.pattern_handlers.lock().unwrap().clear();
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::PUnsubscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (*args, target_node = None, **kwargs))]
    fn ssubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        target_node: Option<Py<PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let _ = target_node;
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.shard_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "ssubscribe() requires at least one shard channel",
            ));
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::SSubscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (*args, target_node = None))]
    fn sunsubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        target_node: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let _ = target_node;
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.shard_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.shard_handlers.lock().unwrap().clear();
        }
        send_command(self, py, |_b, ack_tx| {
            BridgeCommand::SUnsubscribe(names.clone(), ack_tx)
        })?;
        Ok(())
    }

    #[pyo3(signature = (ignore_subscribe_messages = false, timeout = None))]
    fn get_message(
        &self,
        py: Python<'_>,
        ignore_subscribe_messages: bool,
        timeout: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bridge = self
            .bridge
            .as_ref()
            .ok_or_else(|| PubSubError::new_err("pubsub is closed"))?;
        let bridge = bridge.clone();
        let timeout_duration = match timeout {
            None => Some(Duration::from_millis(1)), // non-blocking default
            Some(ref t) => parse_timeout(py, t)?,
        };
        let ignore = ignore_subscribe_messages || self.ignore_subscribe_messages;

        loop {
            let maybe = py.detach(|| {
                get_runtime().block_on(async {
                    let mut rx = bridge.outbound.lock().await;
                    match timeout_duration {
                        Some(d) => time::timeout(d, rx.recv()).await.unwrap_or_default(),
                        None => rx.recv().await,
                    }
                })
            });
            match maybe {
                Some(msg) => {
                    if matches!(msg.kind, PubSubMessageKind::Pong) {
                        continue;
                    }
                    if ignore && msg.kind.is_subscribe_confirmation() {
                        continue;
                    }
                    return msg.into_py_dict(py);
                }
                None => return Ok(py.None()),
            }
        }
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(bridge) = self.bridge.as_ref() {
            bridge
                .closed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = bridge.commands.send(BridgeCommand::Shutdown);
        }
        self.bridge = None;
        Ok(())
    }

    #[getter]
    fn subscribed(&self) -> bool {
        match &self.bridge {
            Some(b) => {
                let g = b.subs.lock().unwrap();
                !g.channels.is_empty() || !g.patterns.is_empty() || !g.shard_channels.is_empty()
            }
            None => false,
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _tb: Py<PyAny>,
    ) -> PyResult<()> {
        self.close()
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn listen(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Extract the bridge Arc and settings while borrowing, then drop the borrow
        // before blocking. This allows another thread to call close() concurrently
        // without hitting PyO3's "already borrowed" error.
        let (bridge, ignore) = {
            let this = slf.borrow();
            match &this.bridge {
                None => return Err(pyo3::exceptions::PyStopIteration::new_err(())),
                Some(b) => (b.clone(), this.ignore_subscribe_messages),
            }
        };
        // Borrow is released here. Block indefinitely waiting for a message.
        loop {
            let maybe = py.detach(|| {
                get_runtime().block_on(async {
                    let mut rx = bridge.outbound.lock().await;
                    rx.recv().await
                })
            });
            match maybe {
                None => {
                    // Sender dropped (bridge closed) → stop iteration.
                    return Err(pyo3::exceptions::PyStopIteration::new_err(()));
                }
                Some(msg) => {
                    if matches!(msg.kind, PubSubMessageKind::Pong) {
                        continue;
                    }
                    if ignore && msg.kind.is_subscribe_confirmation() {
                        continue;
                    }
                    return msg.into_py_dict(py);
                }
            }
        }
    }

    #[pyo3(signature = (sleep_time = 0.0, daemon = false, exception_handler = None))]
    fn run_in_thread(
        slf: Py<Self>,
        py: Python<'_>,
        sleep_time: f64,
        daemon: bool,
        exception_handler: Option<Py<PyAny>>,
    ) -> PyResult<Py<PubSubWorkerThread>> {
        // Validate every active channel/pattern has a handler.
        {
            let this = slf.bind(py).borrow();
            let bridge = this
                .bridge
                .as_ref()
                .ok_or_else(|| PubSubError::new_err("pubsub is closed"))?;
            let subs = bridge.subs.lock().unwrap().clone();
            let chs = this.channel_handlers.lock().unwrap();
            let pats = this.pattern_handlers.lock().unwrap();
            let shs = this.shard_handlers.lock().unwrap();
            for c in &subs.channels {
                if !chs.contains_key(c) {
                    let name = String::from_utf8_lossy(c).to_string();
                    return Err(PubSubError::new_err(format!(
                        "Channel: '{name}' has no handler registered"
                    )));
                }
            }
            for p in &subs.patterns {
                if !pats.contains_key(p) {
                    let name = String::from_utf8_lossy(p).to_string();
                    return Err(PubSubError::new_err(format!(
                        "Pattern: '{name}' has no handler registered"
                    )));
                }
            }
            for s in &subs.shard_channels {
                if !shs.contains_key(s) {
                    let name = String::from_utf8_lossy(s).to_string();
                    return Err(PubSubError::new_err(format!(
                        "Shard Channel: '{name}' has no handler registered"
                    )));
                }
            }
        }

        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let worker = PubSubWorkerThread {
            thread: std::sync::Mutex::new(None),
            running: running.clone(),
        };
        let worker_py = Py::new(py, worker)?;

        let threading = py.import("threading")?;
        let pubsub_py = slf.clone_ref(py);
        let worker_ref = worker_py.clone_ref(py);
        // exception_handler is moved into the closure. Since Py<T> doesn't
        // impl Clone we move the Option directly.
        let exc_h: Option<Py<PyAny>> = exception_handler;

        let py_runner = pyo3::types::PyCFunction::new_closure(
            py,
            None,
            None,
            move |_args, _kwargs| -> PyResult<Py<PyAny>> {
                Python::attach(|py| -> PyResult<Py<PyAny>> {
                    loop {
                        if !running.load(std::sync::atomic::Ordering::SeqCst) {
                            break;
                        }
                        let timeout_val: Py<PyAny> =
                            PyFloat::new(py, sleep_time).into_any().unbind();
                        let res = pubsub_py.bind(py).call_method(
                            "get_message",
                            (),
                            Some(&{
                                let kw = PyDict::new(py);
                                kw.set_item("ignore_subscribe_messages", true)?;
                                kw.set_item("timeout", &timeout_val)?;
                                kw
                            }),
                        );
                        let msg = match res {
                            Ok(m) => m,
                            Err(e) => {
                                if let Some(handler) = exc_h.as_ref() {
                                    let _ = handler.call1(
                                        py,
                                        (
                                            e.into_pyobject(py)?,
                                            pubsub_py.clone_ref(py),
                                            worker_ref.clone_ref(py),
                                        ),
                                    );
                                    continue;
                                } else {
                                    return Err(e);
                                }
                            }
                        };
                        if msg.is_none() {
                            continue;
                        }
                        // Dispatch to handler.
                        let dict = msg.cast::<PyDict>()?;
                        let kind: String = dict.get_item("type")?.unwrap().extract()?;
                        let handler_opt = {
                            let bind_pubsub = pubsub_py.bind(py).borrow();
                            match kind.as_str() {
                                "message" => {
                                    let ch: Vec<u8> =
                                        dict.get_item("channel")?.unwrap().extract()?;
                                    bind_pubsub
                                        .channel_handlers
                                        .lock()
                                        .unwrap()
                                        .get(&ch)
                                        .map(|h| h.clone_ref(py))
                                }
                                "pmessage" => {
                                    let pat: Vec<u8> =
                                        dict.get_item("pattern")?.unwrap().extract()?;
                                    bind_pubsub
                                        .pattern_handlers
                                        .lock()
                                        .unwrap()
                                        .get(&pat)
                                        .map(|h| h.clone_ref(py))
                                }
                                "smessage" => {
                                    let ch: Vec<u8> =
                                        dict.get_item("channel")?.unwrap().extract()?;
                                    bind_pubsub
                                        .shard_handlers
                                        .lock()
                                        .unwrap()
                                        .get(&ch)
                                        .map(|h| h.clone_ref(py))
                                }
                                _ => None,
                            }
                        };
                        if let Some(h) = handler_opt {
                            let msg_unbind = msg.into_any().unbind();
                            if let Err(e) = h.call1(py, (msg_unbind,)) {
                                if let Some(eh) = exc_h.as_ref() {
                                    let _ = eh.call1(
                                        py,
                                        (
                                            e.into_pyobject(py)?,
                                            pubsub_py.clone_ref(py),
                                            worker_ref.clone_ref(py),
                                        ),
                                    );
                                } else {
                                    return Err(e);
                                }
                            }
                        }
                    }
                    Ok(py.None())
                })
            },
        )?;

        let kwargs = PyDict::new(py);
        kwargs.set_item("target", py_runner)?;
        kwargs.set_item("daemon", daemon)?;
        let thread_obj = threading.getattr("Thread")?.call((), Some(&kwargs))?;
        thread_obj.call_method0("start")?;

        {
            let t = worker_py.bind(py).borrow();
            *t.thread.lock().unwrap() = Some(thread_obj.unbind());
        }

        Ok(worker_py)
    }
}

// =========================================================================
// #[pymethods] impl PubSubWorkerThread
// =========================================================================

#[pymethods]
impl PubSubWorkerThread {
    fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    #[pyo3(signature = (timeout = None))]
    fn join(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        let thread = {
            let g = self.thread.lock().unwrap();
            g.as_ref().map(|t| t.clone_ref(py))
        };
        if let Some(t) = thread {
            let kwargs = PyDict::new(py);
            if let Some(s) = timeout {
                kwargs.set_item("timeout", s)?;
            }
            t.bind(py).call_method("join", (), Some(&kwargs))?;
        }
        Ok(())
    }

    fn is_alive(&self, py: Python<'_>) -> PyResult<bool> {
        let g = self.thread.lock().unwrap();
        if let Some(t) = g.as_ref() {
            Ok(t.bind(py).call_method0("is_alive")?.extract()?)
        } else {
            Ok(false)
        }
    }
}

// =========================================================================
// #[pymethods] impl AsyncPubSub
// =========================================================================

#[pymethods]
impl AsyncPubSub {
    #[pyo3(signature = (*args, **kwargs))]
    fn subscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.channel_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "subscribe() requires at least one channel",
            ));
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::Subscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (*args))]
    fn unsubscribe(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.channel_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.channel_handlers.lock().unwrap().clear();
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::Unsubscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn psubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.pattern_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "psubscribe() requires at least one pattern",
            ));
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::PSubscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (*args))]
    fn punsubscribe(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.pattern_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.pattern_handlers.lock().unwrap().clear();
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::PUnsubscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (*args, target_node = None, **kwargs))]
    fn ssubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        target_node: Option<Py<PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = target_node;
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let name = coerce_name(&k)?;
                let mut h = self.shard_handlers.lock().unwrap();
                h.insert(name.clone(), v.clone().unbind());
                names.push(name);
            }
        }
        if names.is_empty() {
            return Err(DataError::new_err(
                "ssubscribe() requires at least one shard channel",
            ));
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::SSubscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (*args, target_node = None))]
    fn sunsubscribe(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        target_node: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = target_node;
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in args.iter() {
            names.push(coerce_name(&item)?);
        }
        if !names.is_empty() {
            let mut h = self.shard_handlers.lock().unwrap();
            for n in &names {
                h.remove(n);
            }
        } else {
            self.shard_handlers.lock().unwrap().clear();
        }
        async_send_command(self, py, |_b, ack_tx| {
            BridgeCommand::SUnsubscribe(names.clone(), ack_tx)
        })
    }

    #[pyo3(signature = (ignore_subscribe_messages = false, timeout = None))]
    fn aget_message(
        &self,
        py: Python<'_>,
        ignore_subscribe_messages: bool,
        timeout: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bridge = self
            .bridge
            .as_ref()
            .ok_or_else(|| PubSubError::new_err("pubsub is closed"))?;
        let bridge = bridge.clone();
        let timeout_duration = match timeout {
            None => Some(Duration::from_millis(1)), // non-blocking default
            Some(ref t) => parse_timeout(py, t)?,
        };
        let ignore = ignore_subscribe_messages || self.ignore_subscribe_messages;

        let (tx, rx) = tokio::sync::oneshot::channel();
        let awaitable = RedisRsAwaitable::new(rx);
        get_runtime().spawn(async move {
            loop {
                let recv = {
                    let mut g = bridge.outbound.lock().await;
                    match timeout_duration {
                        Some(d) => time::timeout(d, g.recv()).await.unwrap_or_default(),
                        None => g.recv().await,
                    }
                };
                let msg = match recv {
                    Some(m) => m,
                    None => {
                        let _ = tx.send(RawResult::Nil);
                        return;
                    }
                };
                if matches!(msg.kind, PubSubMessageKind::Pong) {
                    continue;
                }
                if ignore && msg.kind.is_subscribe_confirmation() {
                    continue;
                }
                let _ = tx.send(RawResult::PubSubMessage(msg));
                return;
            }
        });
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn aclose(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(bridge) = self.bridge.as_ref() {
            bridge
                .closed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = bridge.commands.send(BridgeCommand::Shutdown);
        }
        self.bridge = None;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(RawResult::Nil);
        let awaitable = RedisRsAwaitable::new(rx);
        Ok(awaitable.into_pyobject(py)?.into_any().unbind())
    }

    fn listen(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.bridge.is_none() {
            return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
        }
        // Block indefinitely (timeout=None) so async for truly waits.
        self.aget_message(py, false, None)
    }

    #[getter]
    fn subscribed(&self) -> bool {
        match &self.bridge {
            Some(b) => {
                let g = b.subs.lock().unwrap();
                !g.channels.is_empty() || !g.patterns.is_empty() || !g.shard_channels.is_empty()
            }
            None => false,
        }
    }
}
