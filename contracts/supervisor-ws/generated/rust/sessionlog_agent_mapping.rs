// SessionlogAgentMapping represents a SessionlogAgentMapping model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionlogAgentMapping {
    #[serde(rename="agent_id", skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(rename="parent_tool_use_id", skip_serializing_if = "Option::is_none")]
    pub parent_tool_use_id: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
