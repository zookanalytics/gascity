// Scope represents a Scope model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Scope {
    #[serde(rename="city", skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
}
