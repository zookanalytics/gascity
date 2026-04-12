// ErrorEnvelope represents a ErrorEnvelope model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ErrorEnvelope {
    #[serde(rename="type")]
    pub reserved_type: Box<crate::AnonymousSchema58>,
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="code")]
    pub code: String,
    #[serde(rename="message")]
    pub message: String,
    #[serde(rename="details", skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<crate::FieldError>>,
}
