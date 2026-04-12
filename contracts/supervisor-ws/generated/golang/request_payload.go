
package wscontract

type RequestPayload struct {
  CityPatchRequest `json:"-,omitempty`
  SessionsListRequest `json:"-,omitempty`
  BeadsListRequest `json:"-,omitempty`
  MailListRequest `json:"-,omitempty`
  MailGetRequest `json:"-,omitempty`
  MailReplyRequest `json:"-,omitempty`
  MailSendRequest `json:"-,omitempty`
  EventsListRequest `json:"-,omitempty`
  NameRequest `json:"-,omitempty`
  IdRequest `json:"-,omitempty`
  ProvidersListRequest `json:"-,omitempty`
  SessionSubmitRequest `json:"-,omitempty`
  SessionTranscriptRequest `json:"-,omitempty`
  SubscriptionStartRequest `json:"-,omitempty`
  SubscriptionStopRequest `json:"-,omitempty`
}