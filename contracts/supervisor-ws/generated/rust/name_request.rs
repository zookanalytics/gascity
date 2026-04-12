// NameRequest represents a NameRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NameRequest {
    #[serde(rename="name")]
    pub name: String,
}
