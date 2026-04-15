// ApiOutputTurn represents a ApiOutputTurn model.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiOutputTurn {
    #[serde(rename="role", skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(rename="text", skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(rename="timestamp", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(rename="additionalProperties", skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<std::collections::HashMap<String, serde_json::Value>>,
}
