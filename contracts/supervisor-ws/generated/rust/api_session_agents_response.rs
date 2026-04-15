// ApiSessionAgentsResponse represents a ApiSessionAgentsResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiSessionAgentsResponse {
    #[serde(rename="agents", skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<crate::SessionlogAgentMapping>>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
