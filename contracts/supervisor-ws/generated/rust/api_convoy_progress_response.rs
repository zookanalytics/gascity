// ApiConvoyProgressResponse represents a ApiConvoyProgressResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiConvoyProgressResponse {
    #[serde(rename="closed", skip_serializing_if = "Option::is_none")]
    pub closed: Option<i32>,
    #[serde(rename="total", skip_serializing_if = "Option::is_none")]
    pub total: Option<i32>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
