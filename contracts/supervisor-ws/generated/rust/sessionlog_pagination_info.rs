// SessionlogPaginationInfo represents a SessionlogPaginationInfo model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionlogPaginationInfo {
    #[serde(rename="has_older_messages", skip_serializing_if = "Option::is_none")]
    pub has_older_messages: Option<bool>,
    #[serde(rename="returned_message_count", skip_serializing_if = "Option::is_none")]
    pub returned_message_count: Option<i32>,
    #[serde(rename="total_compactions", skip_serializing_if = "Option::is_none")]
    pub total_compactions: Option<i32>,
    #[serde(rename="total_message_count", skip_serializing_if = "Option::is_none")]
    pub total_message_count: Option<i32>,
    #[serde(rename="truncated_before_message", skip_serializing_if = "Option::is_none")]
    pub truncated_before_message: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
