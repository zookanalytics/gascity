// MailSendRequest represents a MailSendRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MailSendRequest {
    #[serde(rename="rig", skip_serializing_if = "Option::is_none")]
    pub rig: Option<String>,
    #[serde(rename="from", skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(rename="to")]
    pub to: String,
    #[serde(rename="subject")]
    pub subject: String,
    #[serde(rename="body", skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}
