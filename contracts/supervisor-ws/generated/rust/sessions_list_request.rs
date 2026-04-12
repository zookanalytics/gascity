// SessionsListRequest represents a SessionsListRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SessionsListRequest {
    #[serde(rename="state", skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(rename="template", skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(rename="peek", skip_serializing_if = "Option::is_none")]
    pub peek: Option<bool>,
}
