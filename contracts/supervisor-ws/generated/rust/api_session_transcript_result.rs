// ApiSessionTranscriptResult represents a ApiSessionTranscriptResult model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiSessionTranscriptResult {
    #[serde(rename="format", skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="messages", skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<Std::collections::HashMap<String, serde_json::Value>>>,
    #[serde(rename="pagination", skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Box<crate::SessionlogPaginationInfo>>,
    #[serde(rename="template", skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(rename="turns", skip_serializing_if = "Option::is_none")]
    pub turns: Option<Vec<crate::ApiOutputTurn>>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
