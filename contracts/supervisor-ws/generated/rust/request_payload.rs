// RequestPayload represents a union of types: CityPatchRequest, SessionsListRequest, BeadsListRequest, MailListRequest, MailGetRequest, MailReplyRequest, MailSendRequest, EventsListRequest, NameRequest, IdRequest, ProvidersListRequest, SessionSubmitRequest, SessionTranscriptRequest, SubscriptionStartRequest, SubscriptionStopRequest
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub enum RequestPayload {
    #[serde(rename="CityPatchRequest")]
    CityPatchRequest(crate::CityPatchRequest),
    #[serde(rename="SessionsListRequest")]
    SessionsListRequest(crate::SessionsListRequest),
    #[serde(rename="BeadsListRequest")]
    BeadsListRequest(crate::BeadsListRequest),
    #[serde(rename="MailListRequest")]
    MailListRequest(crate::MailListRequest),
    #[serde(rename="MailGetRequest")]
    MailGetRequest(crate::MailGetRequest),
    #[serde(rename="MailReplyRequest")]
    MailReplyRequest(crate::MailReplyRequest),
    #[serde(rename="MailSendRequest")]
    MailSendRequest(crate::MailSendRequest),
    #[serde(rename="EventsListRequest")]
    EventsListRequest(crate::EventsListRequest),
    #[serde(rename="NameRequest")]
    NameRequest(crate::NameRequest),
    #[serde(rename="IdRequest")]
    IdRequest(crate::IdRequest),
    #[serde(rename="ProvidersListRequest")]
    ProvidersListRequest(crate::ProvidersListRequest),
    #[serde(rename="SessionSubmitRequest")]
    SessionSubmitRequest(crate::SessionSubmitRequest),
    #[serde(rename="SessionTranscriptRequest")]
    SessionTranscriptRequest(crate::SessionTranscriptRequest),
    #[serde(rename="SubscriptionStartRequest")]
    SubscriptionStartRequest(crate::SubscriptionStartRequest),
    #[serde(rename="SubscriptionStopRequest")]
    SubscriptionStopRequest(crate::SubscriptionStopRequest),
}

