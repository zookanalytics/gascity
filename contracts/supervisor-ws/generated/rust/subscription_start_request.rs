// SubscriptionStartRequest represents a SubscriptionStartRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SubscriptionStartRequest {
    #[serde(rename="kind")]
    pub kind: Box<crate::AnonymousSchema43>,
    #[serde(rename="id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename="cursor", skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(rename="format", skip_serializing_if = "Option::is_none")]
    pub format: Option<Box<crate::AnonymousSchema46>>,
}
