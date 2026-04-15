// ApiMutationStatusIdResponse represents a ApiMutationStatusIdResponse model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiMutationStatusIdResponse {
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
