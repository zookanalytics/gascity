// MailReplyRequest represents a MailReplyRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MailReplyRequest {
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="rig", skip_serializing_if = "Option::is_none")]
    pub rig: Option<String>,
    #[serde(rename="from")]
    pub from: String,
    #[serde(rename="subject")]
    pub subject: String,
    #[serde(rename="body")]
    pub body: String,
}
