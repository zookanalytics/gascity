// MailListRequest represents a MailListRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MailListRequest {
    #[serde(rename="agent", skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(rename="status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(rename="rig", skip_serializing_if = "Option::is_none")]
    pub rig: Option<String>,
}
