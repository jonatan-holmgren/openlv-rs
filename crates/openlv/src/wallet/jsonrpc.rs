#[derive(Debug, serde::Serialize)]
pub struct JsonRpcRequest<'a, T: serde::Serialize> {
    pub jsonrpc: &'a str,
    pub method: &'a str,
    pub params: T,
    pub id: u64,
}

#[derive(Debug, serde::Deserialize)]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(default)]
    pub result: Option<T>,
    #[serde(default)]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, serde::Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

impl<'a, T: serde::Serialize> JsonRpcRequest<'a, T> {
    pub fn new(method: &'a str, params: T, id: u64) -> Self {
        Self {
            jsonrpc: "2.0",
            method,
            params,
            id,
        }
    }
}
