// SessionSubmitRequest represents a SessionSubmitRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SessionSubmitRequest {
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="message")]
    pub message: String,
    #[serde(rename="intent", skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
}
