// IdRequest represents a IdRequest model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IdRequest {
    #[serde(rename="id")]
    pub id: String,
}
