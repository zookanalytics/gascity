// ApiSessionAgentDetailResponse represents a ApiSessionAgentDetailResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiSessionAgentDetailResponse {
    #[serde(rename="messages", skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<Serde_json::Value>>,
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
