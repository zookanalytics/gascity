// ProvidersListRequest represents a ProvidersListRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProvidersListRequest {
    #[serde(rename="view", skip_serializing_if = "Option::is_none")]
    pub view: Option<Box<crate::AnonymousSchema35>>,
}
