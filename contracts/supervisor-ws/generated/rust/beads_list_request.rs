// BeadsListRequest represents a BeadsListRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BeadsListRequest {
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="type", skip_serializing_if = "Option::is_none")]
    pub reserved_type: Option<String>,
    #[serde(rename="label", skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(rename="assignee", skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(rename="rig", skip_serializing_if = "Option::is_none")]
    pub rig: Option<String>,
}
