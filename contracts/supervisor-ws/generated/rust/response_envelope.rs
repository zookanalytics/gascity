// ResponseEnvelope represents a ResponseEnvelope model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResponseEnvelope {
    #[serde(rename="type")]
    pub reserved_type: Box<crate::AnonymousSchema54>,
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="index", skip_serializing_if = "Option::is_none")]
    pub index: Option<i32>,
    #[serde(rename="result", skip_serializing_if = "Option::is_none")]
    pub result: Option<std::collections::HashMap<String, serde_json::Value>>,
}
