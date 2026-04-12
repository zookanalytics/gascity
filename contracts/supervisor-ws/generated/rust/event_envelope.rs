// EventEnvelope represents a EventEnvelope model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventEnvelope {
    #[serde(rename="type")]
    pub reserved_type: Box<crate::AnonymousSchema65>,
    #[serde(rename="subscription_id")]
    pub subscription_id: String,
    #[serde(rename="event_type")]
    pub event_type: String,
    #[serde(rename="index", skip_serializing_if = "Option::is_none")]
    pub index: Option<i32>,
    #[serde(rename="cursor", skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(rename="payload", skip_serializing_if = "Option::is_none")]
    pub payload: Option<std::collections::HashMap<String, serde_json::Value>>,
}
