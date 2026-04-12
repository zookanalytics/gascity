// EventsListRequest represents a EventsListRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EventsListRequest {
    #[serde(rename="type", skip_serializing_if = "Option::is_none")]
    pub reserved_type: Option<String>,
    #[serde(rename="actor", skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(rename="since", skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}
