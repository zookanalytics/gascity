// MailGetRequest represents a MailGetRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MailGetRequest {
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="rig", skip_serializing_if = "Option::is_none")]
    pub rig: Option<String>,
}
