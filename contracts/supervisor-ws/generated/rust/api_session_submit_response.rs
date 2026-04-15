// ApiSessionSubmitResponse represents a ApiSessionSubmitResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiSessionSubmitResponse {
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="intent", skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
    #[serde(rename="queued", skip_serializing_if = "Option::is_none")]
    pub queued: Option<bool>,
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
