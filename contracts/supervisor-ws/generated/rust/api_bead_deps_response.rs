// ApiBeadDepsResponse represents a ApiBeadDepsResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiBeadDepsResponse {
    #[serde(rename="children", skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<crate::BeadsBead>>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
