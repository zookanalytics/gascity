// HelloEnvelope represents a HelloEnvelope model.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct HelloEnvelope {
    #[serde(rename="type")]
    pub reserved_type: Box<crate::AnonymousSchema48>,
    #[serde(rename="protocol")]
    pub protocol: Box<crate::AnonymousSchema49>,
    #[serde(rename="server_role")]
    pub server_role: Box<crate::AnonymousSchema50>,
    #[serde(rename="read_only")]
    pub read_only: bool,
    #[serde(rename="capabilities")]
    pub capabilities: Vec<String>,
}
