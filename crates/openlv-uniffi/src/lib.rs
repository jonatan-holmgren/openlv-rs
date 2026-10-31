//! UniFFI bridge for the openlv session API.

use parking_lot::Mutex;
use serde::Deserialize;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum BridgeError {
    #[error("{message}")]
    Failure { kind: String, message: String },
}

fn failure(kind: &str, message: impl std::fmt::Display) -> BridgeError {
    BridgeError::Failure {
        kind: kind.to_owned(),
        message: message.to_string(),
    }
}

impl From<openlv::OpenLvError> for BridgeError {
    fn from(value: openlv::OpenLvError) -> Self {
        use openlv::OpenLvError as E;
        let kind = match &value {
            E::InvalidUri(_) => "invalid_uri",
            E::Signaling(_) => "signaling",
            E::Transport(_) => "transport",
            E::Session(_)
            | E::SessionClosed
            | E::RequestLimitReached
            | E::RequestFailed
            | E::InvalidRequestTimeout
            | E::ResponseChannelClosed => "session",
            E::Encryption(_) => "encryption",
            E::WireFrame(_) => "wire_frame",
            E::ProtocolViolation(_) => "protocol_violation",
            E::UnsupportedProtocol(_) => "unsupported_protocol",
            E::NoConnection => "no_connection",
            E::RequestTimeout(_) => "request_timeout",
            E::Other(_) => "other",
        };
        failure(kind, value)
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Tokio runtime")
    });
    &RUNTIME
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn on_runtime<T: Send + 'static>(
    future: impl Future<Output = Result<T, BridgeError>> + Send + 'static,
) -> Result<T, BridgeError> {
    let mut task = AbortOnDrop(runtime().spawn(future));
    (&mut task.0).await.map_err(|e| failure("other", e))?
}

#[derive(Deserialize)]
struct PeerInfo {
    identity: String,
    name: String,
    icon: Option<String>,
}
impl PeerInfo {
    fn validate(self) -> Result<openlv::PeerInfo, BridgeError> {
        let info = openlv::PeerInfo {
            identity: self.identity,
            name: self.name,
            icon: self.icon,
        };
        info.validate()
            .map_err(|reason| failure("session", reason))?;
        Ok(info)
    }
}

#[derive(Deserialize)]
struct SessionOptions {
    session_id: Option<String>,
    protocol: Option<String>,
    server: Option<String>,
    handshake_key: Option<String>,
    peer_info: Option<PeerInfo>,
}

fn parse_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, BridgeError> {
    serde_json::from_str(value).map_err(|e| failure("other", e))
}

struct Request {
    id: u64,
    message: String,
}
struct Requests {
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<String>>>,
    sender: mpsc::UnboundedSender<Request>,
    receiver: tokio::sync::Mutex<mpsc::UnboundedReceiver<Request>>,
    stop: watch::Sender<bool>,
}

// Remove entries if the core cancels a request while the handler is pending.
struct PendingRequest {
    requests: Arc<Requests>,
    id: u64,
}
impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.requests.pending.lock().remove(&self.id);
    }
}

impl Requests {
    fn new() -> Arc<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            next_id: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            stop,
        })
    }
    async fn handle(
        self: Arc<Self>,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, openlv::OpenLvError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock();
            if *self.stop.borrow() {
                return Err(openlv::OpenLvError::SessionClosed);
            }
            pending.insert(id, tx);
            if self
                .sender
                .send(Request {
                    id,
                    message: value.to_string(),
                })
                .is_err()
            {
                pending.remove(&id);
                return Err(openlv::OpenLvError::SessionClosed);
            }
        }
        let _pending = PendingRequest { requests: self, id };
        let reply = rx.await.map_err(|_| openlv::OpenLvError::SessionClosed)?;
        serde_json::from_str(&reply).map_err(Into::into)
    }
    fn stop(&self) {
        let mut pending = self.pending.lock();
        self.stop.send_replace(true);
        pending.clear();
    }
    fn reply(&self, id: u64, result: String) -> Result<(), BridgeError> {
        let sender = self
            .pending
            .lock()
            .remove(&id)
            .ok_or_else(|| failure("session", "Request is no longer pending"))?;
        sender
            .send(result)
            .map_err(|_| failure("session", "Request is no longer pending"))
    }
}

#[derive(uniffi::Object)]
pub struct Session {
    inner: openlv::Session,
    requests: Arc<Requests>,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.requests.stop();
    }
}

#[uniffi::export]
pub async fn create_dapp(options_json: String) -> Result<Arc<Session>, BridgeError> {
    on_runtime(async move {
        let options: SessionOptions = parse_json(&options_json)?;
        let peer = options.peer_info.map(PeerInfo::validate).transpose()?;
        let mut config = openlv::dapp();
        if let Some(id) = options.session_id {
            config = config.session_id(id);
        }
        if let Some(protocol) = options.protocol {
            config = config.protocol(protocol);
        }
        if let Some(server) = options.server {
            config = config.server(server);
        }
        if let Some(key) = options.handshake_key {
            config = config.handshake_key(
                openlv::encryption::HandshakeKey::from_hex(&key).map_err(BridgeError::from)?,
            );
        }
        if let Some(peer) = peer {
            config = config.info(peer);
        }
        let requests = Requests::new();
        let handler = requests.clone();
        let inner = config
            .on_request(move |value| handler.clone().handle(value))
            .await
            .map_err(BridgeError::from)?;
        Ok(Arc::new(Session { inner, requests }))
    })
    .await
}

#[uniffi::export]
pub async fn create_wallet(
    uri: String,
    peer_info_json: String,
) -> Result<Arc<Session>, BridgeError> {
    on_runtime(async move {
        let peer: Option<PeerInfo> = parse_json(&peer_info_json)?;
        let mut config = openlv::wallet(&uri);
        if let Some(peer) = peer {
            config = config.info(peer.validate()?);
        }
        let requests = Requests::new();
        let handler = requests.clone();
        let inner = config
            .on_request(move |value| handler.clone().handle(value))
            .await
            .map_err(BridgeError::from)?;
        Ok(Arc::new(Session { inner, requests }))
    })
    .await
}

#[uniffi::export]
pub async fn parse_session_uri(url: String) -> Result<String, BridgeError> {
    let openlv::SessionUri::Version1(uri) =
        openlv::SessionUri::from_url(&url).map_err(BridgeError::from)?;
    Ok(serde_json::json!({"session_id": uri.session_id, "key_hash": uri.key_hash.0,
        "handshake_key": uri.shared_key.to_hex().to_string(), "protocol": uri.signaling_protocol.to_string(),
        "server": uri.signaling_server}).to_string())
}

#[uniffi::export]
pub async fn generate_session_id() -> String {
    openlv::url::generate_session_id()
}

fn state_json(state: openlv::SessionStateObject) -> String {
    let status = match state.status {
        openlv::SessionState::Created => "created",
        openlv::SessionState::Signaling => "signaling",
        openlv::SessionState::Ready => "ready",
        openlv::SessionState::Linking => "linking",
        openlv::SessionState::Connected => "connected",
        openlv::SessionState::Disconnected => "disconnected",
    };
    let signaling = state.signaling.map(|s| match s {
        openlv::SignalState::Standby => "standby",
        openlv::SignalState::Connecting => "connecting",
        openlv::SignalState::Ready => "ready",
        openlv::SignalState::Handshake => "handshake",
        openlv::SignalState::HandshakePartial => "handshake_partial",
        openlv::SignalState::Encrypted => "encrypted",
        openlv::SignalState::Error => "error",
    });
    let peer = state
        .peer_info
        .map(|p| serde_json::json!({"identity": p.identity, "name": p.name, "icon": p.icon}));
    serde_json::json!({"status": status, "signaling": signaling, "peer": peer, "error": state.error}).to_string()
}

#[uniffi::export]
impl Session {
    pub async fn uri(&self) -> String {
        self.inner.uri().to_string()
    }
    pub async fn session_id(&self) -> String {
        let openlv::SessionUri::Version1(uri) = self.inner.uri();
        uri.session_id.clone()
    }
    pub async fn is_host(&self) -> bool {
        self.inner.is_host()
    }
    pub async fn state(&self) -> String {
        state_json(self.inner.state())
    }
    pub async fn subscribe_states(&self) -> Arc<StateSubscription> {
        Arc::new(StateSubscription {
            inner: self.inner.clone(),
            receiver: Arc::new(tokio::sync::Mutex::new(self.inner.subscribe_state())),
        })
    }
    pub async fn connect(&self) -> Result<(), BridgeError> {
        let session = self.inner.clone();
        on_runtime(async move { session.connect().await.map_err(Into::into) }).await
    }
    pub async fn wait_for_link(&self) -> Result<(), BridgeError> {
        let session = self.inner.clone();
        on_runtime(async move { session.wait_for_link().await.map_err(Into::into) }).await
    }
    pub async fn send(&self, message: String) -> Result<String, BridgeError> {
        let session = self.inner.clone();
        on_runtime(async move {
            let value = parse_json(&message)?;
            let response = session.send(value).await.map_err(BridgeError::from)?;
            serde_json::to_string(&response).map_err(|e| failure("other", e))
        })
        .await
    }
    pub async fn send_with_timeouts(
        &self,
        message: String,
        ack_timeout_ms: u64,
        response_timeout_ms: u64,
    ) -> Result<String, BridgeError> {
        let session = self.inner.clone();
        on_runtime(async move {
            let value = parse_json(&message)?;
            let response = session
                .send_with_timeouts(value, ack_timeout_ms, response_timeout_ms)
                .await
                .map_err(BridgeError::from)?;
            serde_json::to_string(&response).map_err(|e| failure("other", e))
        })
        .await
    }
    pub async fn next_request(&self) -> Result<Option<String>, BridgeError> {
        let requests = self.requests.clone();
        let session = self.inner.clone();
        on_runtime(async move {
            let mut stop = requests.stop.subscribe();
            let mut receiver = requests.receiver.lock().await;
            loop {
                if *stop.borrow() { return Ok(None); }
                tokio::select! {
                    biased;
                    _ = session.closed() => return Ok(None),
                    _ = stop.changed() => return Ok(None),
                    request = receiver.recv() => match request {
                        Some(request) if requests.pending.lock().contains_key(&request.id) => {
                            return Ok(Some(serde_json::json!({"id": request.id, "message": request.message}).to_string()));
                        }
                        Some(_) => continue,
                        None => return Ok(None),
                    }
                }
            }
        }).await
    }
    pub fn respond(&self, id: u64, response: String) -> Result<(), BridgeError> {
        self.requests.reply(id, response)
    }
    pub async fn shutdown(&self) -> Result<(), BridgeError> {
        self.requests.stop();
        let session = self.inner.clone();
        on_runtime(async move { session.close().await.map_err(Into::into) }).await
    }
}

#[derive(uniffi::Object)]
pub struct StateSubscription {
    inner: openlv::Session,
    receiver: Arc<tokio::sync::Mutex<broadcast::Receiver<openlv::SessionStateObject>>>,
}

#[uniffi::export]
impl StateSubscription {
    pub async fn next_state(&self) -> Result<Option<String>, BridgeError> {
        let session = self.inner.clone();
        let receiver = self.receiver.clone();
        on_runtime(async move {
            let mut receiver = receiver.lock().await;
            loop {
                tokio::select! {
                    biased;
                    _ = session.closed() => return Ok(None),
                    state = receiver.recv() => match state {
                        Ok(state) => return Ok(Some(state_json(state))),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return Ok(None),
                    }
                }
            }
        })
        .await
    }
}
