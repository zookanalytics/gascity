// RequestEnvelope represents a RequestEnvelope model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RequestEnvelope {
    #[serde(rename="type")]
    pub reserved_type: Box<crate::AnonymousSchema1>,
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="action")]
    pub action: Box<crate::AnonymousSchema3>,
    #[serde(rename="idempotency_key", skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(rename="scope", skip_serializing_if = "Option::is_none")]
    pub scope: Option<Box<crate::Scope>>,
    #[serde(rename="payload", skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<crate::RequestPayload>>,
}
