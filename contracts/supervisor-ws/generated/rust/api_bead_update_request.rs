// ApiBeadUpdateRequest represents a ApiBeadUpdateRequest model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiBeadUpdateRequest {
    #[serde(rename="assignee", skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(rename="description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(rename="metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    #[serde(rename="priority", skip_serializing_if = "Option::is_none")]
    pub priority: Option<std::collections::HashMap<String, serde_json::Value>>,
    #[serde(rename="remove_labels", skip_serializing_if = "Option::is_none")]
    pub remove_labels: Option<Vec<String>>,
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="title", skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(rename="type", skip_serializing_if = "Option::is_none")]
    pub reserved_type: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
