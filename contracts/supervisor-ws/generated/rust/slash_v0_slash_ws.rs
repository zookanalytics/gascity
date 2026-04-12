// SlashV0SlashWs represents a union of types: HelloEnvelope, ResponseEnvelope, ErrorEnvelope, EventEnvelope
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub enum SlashV0SlashWs {
    #[serde(rename="HelloEnvelope")]
    HelloEnvelope(crate::HelloEnvelope),
    #[serde(rename="ResponseEnvelope")]
    ResponseEnvelope(crate::ResponseEnvelope),
    #[serde(rename="ErrorEnvelope")]
    ErrorEnvelope(crate::ErrorEnvelope),
    #[serde(rename="EventEnvelope")]
    EventEnvelope(crate::EventEnvelope),
}

