use std::task::{Context, Poll};

use alloy::{
    rpc::{
        client::RpcClient,
        json_rpc::{RequestPacket, Response, ResponsePacket, SerializedRequest},
    },
    transports::{TransportError, TransportErrorKind, TransportFut},
};
use tower::Service;

/// A transport layer for alloy using an OpenLV [`Session`] as the underlying
/// transport.
#[derive(Clone)]
pub struct Transport {
    session: crate::Session,
}

/// Creates a new alloy `RpcClient` using the given session as the transport layer.
///
/// ```no_run
/// # let session = openlv::dapp().on_request(|_| async move Ok(json!({"result": "success"})).await.unwrap();
/// let client = openlv::provider::rpc_client(session);
/// let provider = alloy::providers::ProviderBuilder::new().connect_client(client);
/// ```
pub fn rpc_client(session: crate::Session) -> RpcClient {
    let transport = Transport::new(session);
    RpcClient::new(transport, false)
}

impl Transport {
    pub fn new(session: crate::Session) -> Self {
        Self { session }
    }
}

impl Service<RequestPacket> for Transport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let session = self.session.clone();
        Box::pin(async move {
            match req {
                RequestPacket::Single(r) => {
                    let resp = dispatch_one(&session, r).await?;
                    Ok(ResponsePacket::Single(resp))
                }
                RequestPacket::Batch(rs) => {
                    let mut out = Vec::with_capacity(rs.len());
                    for r in rs {
                        out.push(dispatch_one(&session, r).await?);
                    }
                    Ok(ResponsePacket::Batch(out))
                }
            }
        })
    }
}

async fn dispatch_one(
    session: &crate::Session,
    req: SerializedRequest,
) -> Result<Response, TransportError> {
    let id = req.id().clone();
    let req_val =
        serde_json::from_str(req.into_serialized().get()).map_err(TransportErrorKind::custom)?;

    let resp_val = session
        .send(req_val)
        .await
        .map_err(TransportErrorKind::custom)?;

    // Handlers return the bare result value on success, or `{"error": {...}}`
    // on failure not a full JSON-RPC response envelope.
    //
    // TODO: Replace this after https://github.com/open-lavatory/openlv-rs/issues/7
    let envelope = match resp_val {
        serde_json::Value::Object(ref map) if map.contains_key("error") => {
            serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": map["error"] })
        }
        other => serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": other }),
    };

    serde_json::from_value(envelope).map_err(TransportErrorKind::custom)
}
