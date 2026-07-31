use std::task::{Context, Poll};

use alloy_json_rpc_2::{RequestPacket, Response, ResponsePacket, SerializedRequest};
use alloy_rpc_client_2::RpcClient;
use alloy_transport_2::{TransportError, TransportErrorKind, TransportFut};
use tower::Service;

#[derive(Clone)]
pub struct Transport {
    session: crate::Session,
}

pub fn rpc_client(session: crate::Session) -> RpcClient {
    RpcClient::new(Transport::new(session), false)
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
                RequestPacket::Single(request) => Ok(ResponsePacket::Single(
                    dispatch_one(&session, request).await?,
                )),
                RequestPacket::Batch(requests) => {
                    let mut responses = Vec::with_capacity(requests.len());
                    for request in requests {
                        responses.push(dispatch_one(&session, request).await?);
                    }
                    Ok(ResponsePacket::Batch(responses))
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
    let envelope = response_envelope(id, resp_val);

    serde_json::from_value(envelope).map_err(TransportErrorKind::custom)
}

fn response_envelope(id: alloy_json_rpc_2::Id, value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(ref map) if map.contains_key("error") => {
            serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": map["error"] })
        }
        result => serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result }),
    }
}
