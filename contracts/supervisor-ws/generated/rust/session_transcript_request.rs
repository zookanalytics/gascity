// SessionTranscriptRequest represents a SessionTranscriptRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SessionTranscriptRequest {
    #[serde(rename="id")]
    pub id: String,
    #[serde(rename="tail", skip_serializing_if = "Option::is_none")]
    pub tail: Option<i32>,
    #[serde(rename="before", skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(rename="format", skip_serializing_if = "Option::is_none")]
    pub format: Option<Box<crate::AnonymousSchema42>>,
}
