use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use alloy::{
    consensus::{SignableTransaction, Signed},
    eips::Decodable2718,
    network::{Network, NetworkWallet},
    primitives::{Address, Bytes},
    rpc::types::TransactionRequest,
    signers::Signature,
};

use crate::{Session, wallet::jsonrpc::JsonRpcRequest};

mod jsonrpc;

#[derive(Clone)]
pub struct Wallet {
    session: Session,
    id: Arc<AtomicU64>,
    default: Address,
    addresses: Vec<Address>,
}

#[derive(Debug, thiserror::Error)]
pub enum WalletError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("openlv error: {0}")]
    OpenLV(#[from] crate::OpenLvError),
    #[error("no addresses returned from wallet")]
    NoAddresses,
}

type RequestAccountsResp = Vec<Address>;
type SignTransactionResponse = SignTransactionResult;

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct SignTransactionResult {
    raw: Bytes,
}

impl Wallet {
    pub async fn new(session: Session) -> Result<Self, WalletError> {
        let id = Arc::new(AtomicU64::new(1));

        let req = JsonRpcRequest::new("eth_requestAccounts", (), id.fetch_add(1, Ordering::SeqCst));
        let req_var = serde_json::to_value(req)?;

        let resp_var = session.send(req_var).await?;
        let addresses: RequestAccountsResp = serde_json::from_value(resp_var)?;

        let default = addresses
            .first()
            .copied()
            .ok_or_else(|| WalletError::NoAddresses)?;

        Ok(Self {
            session,
            id,
            default,
            addresses,
        })
    }
}

impl<N: Network> NetworkWallet<N> for Wallet
where
    N::TxEnvelope: From<Signed<N::UnsignedTx>>,
    N::UnsignedTx: SignableTransaction<Signature>,
    TransactionRequest: From<N::UnsignedTx>,
{
    fn default_signer_address(&self) -> alloy::primitives::Address {
        self.default
    }

    fn has_signer_for(&self, address: &alloy::primitives::Address) -> bool {
        self.addresses.contains(address)
    }

    fn signer_addresses(&self) -> impl Iterator<Item = alloy::primitives::Address> {
        self.addresses.clone().into_iter()
    }

    async fn sign_transaction_from(
        &self,
        sender: Address,
        tx: N::UnsignedTx,
    ) -> alloy::signers::Result<N::TxEnvelope> {
        if !self.addresses.contains(&sender) {
            return Err(alloy::signers::Error::message(format!(
                "no signer for address {sender}"
            )));
        }

        let mut tx_req: TransactionRequest = tx.into();
        tx_req.from = Some(sender);

        let req = JsonRpcRequest::new(
            "eth_signTransaction",
            (tx_req,),
            self.id.fetch_add(1, Ordering::SeqCst),
        );
        let req_val = serde_json::to_value(req).map_err(|e| {
            alloy::signers::Error::message(format!("failed to serialize request to OpenLV: {e}"))
        })?;

        let resp_value = self.session.send(req_val).await.map_err(|e| {
            alloy::signers::Error::message(format!("failed to send request to OpenLV: {e}"))
        })?;
        let resp: SignTransactionResponse = serde_json::from_value(resp_value).map_err(|e| {
            alloy::signers::Error::message(format!(
                "failed to deserialize response from OpenLV: {e}"
            ))
        })?;

        let mut raw = resp.raw.as_ref();
        let envelope = N::TxEnvelope::decode_2718(&mut raw)
            .map_err(|e| alloy::signers::Error::message(format!("failed to decode raw tx: {e}")))?;

        Ok(envelope)
    }
}

impl std::fmt::Debug for Wallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenLVWallet").finish()
    }
}
