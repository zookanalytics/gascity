// ApiConvoySnapshotResponse represents a ApiConvoySnapshotResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiConvoySnapshotResponse {
    #[serde(rename="children", skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<crate::BeadsBead>>,
    #[serde(rename="convoy", skip_serializing_if = "Option::is_none")]
    pub convoy: Option<Box<crate::BeadsBead>>,
    #[serde(rename="progress", skip_serializing_if = "Option::is_none")]
    pub progress: Option<Box<crate::ApiConvoyProgressResponse>>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
