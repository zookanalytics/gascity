// FieldError represents a FieldError model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FieldError {
    #[serde(rename="field")]
    pub field: String,
    #[serde(rename="message")]
    pub message: String,
}
