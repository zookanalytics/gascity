// CityPatchRequest represents a CityPatchRequest model.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CityPatchRequest {
    #[serde(rename="suspended", skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
}
