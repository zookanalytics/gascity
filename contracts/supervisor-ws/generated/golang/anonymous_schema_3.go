
package wscontract
import (  
  "encoding/json"
)
type AnonymousSchema_3 uint

const (
  AnonymousSchema_3HealthDotGet AnonymousSchema_3 = iota
  AnonymousSchema_3CitiesDotList
  AnonymousSchema_3CityDotGet
  AnonymousSchema_3CityDotPatch
  AnonymousSchema_3ConfigDotGet
  AnonymousSchema_3ConfigDotExplain
  AnonymousSchema_3ConfigDotValidate
  AnonymousSchema_3StatusDotGet
  AnonymousSchema_3SessionsDotList
  AnonymousSchema_3BeadsDotList
  AnonymousSchema_3BeadsDotReady
  AnonymousSchema_3BeadDotGet
  AnonymousSchema_3BeadDotCreate
  AnonymousSchema_3BeadDotClose
  AnonymousSchema_3BeadDotUpdate
  AnonymousSchema_3MailDotList
  AnonymousSchema_3MailDotGet
  AnonymousSchema_3MailDotRead
  AnonymousSchema_3MailDotReply
  AnonymousSchema_3MailDotSend
  AnonymousSchema_3EventsDotList
  AnonymousSchema_3RigsDotList
  AnonymousSchema_3ConvoysDotList
  AnonymousSchema_3ConvoyDotGet
  AnonymousSchema_3SlingDotRun
  AnonymousSchema_3ServicesDotList
  AnonymousSchema_3ServiceDotGet
  AnonymousSchema_3ServiceDotRestart
  AnonymousSchema_3PacksDotList
  AnonymousSchema_3ProvidersDotList
  AnonymousSchema_3ProviderDotGet
  AnonymousSchema_3AgentDotSuspend
  AnonymousSchema_3AgentDotResume
  AnonymousSchema_3RigDotSuspend
  AnonymousSchema_3RigDotResume
  AnonymousSchema_3RigDotRestart
  AnonymousSchema_3SessionDotKill
  AnonymousSchema_3SessionDotPending
  AnonymousSchema_3SessionDotSubmit
  AnonymousSchema_3SessionDotTranscript
  AnonymousSchema_3SubscriptionDotStart
  AnonymousSchema_3SubscriptionDotStop
)

// Value returns the value of the enum.
func (op AnonymousSchema_3) Value() any {
	if op >= AnonymousSchema_3(len(AnonymousSchema_3Values)) {
		return nil
	}
	return AnonymousSchema_3Values[op]
}

var AnonymousSchema_3Values = []any{"health.get","cities.list","city.get","city.patch","config.get","config.explain","config.validate","status.get","sessions.list","beads.list","beads.ready","bead.get","bead.create","bead.close","bead.update","mail.list","mail.get","mail.read","mail.reply","mail.send","events.list","rigs.list","convoys.list","convoy.get","sling.run","services.list","service.get","service.restart","packs.list","providers.list","provider.get","agent.suspend","agent.resume","rig.suspend","rig.resume","rig.restart","session.kill","session.pending","session.submit","session.transcript","subscription.start","subscription.stop"}
var ValuesToAnonymousSchema_3 = map[any]AnonymousSchema_3{
  AnonymousSchema_3Values[AnonymousSchema_3HealthDotGet]: AnonymousSchema_3HealthDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3CitiesDotList]: AnonymousSchema_3CitiesDotList,
  AnonymousSchema_3Values[AnonymousSchema_3CityDotGet]: AnonymousSchema_3CityDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3CityDotPatch]: AnonymousSchema_3CityDotPatch,
  AnonymousSchema_3Values[AnonymousSchema_3ConfigDotGet]: AnonymousSchema_3ConfigDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3ConfigDotExplain]: AnonymousSchema_3ConfigDotExplain,
  AnonymousSchema_3Values[AnonymousSchema_3ConfigDotValidate]: AnonymousSchema_3ConfigDotValidate,
  AnonymousSchema_3Values[AnonymousSchema_3StatusDotGet]: AnonymousSchema_3StatusDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3SessionsDotList]: AnonymousSchema_3SessionsDotList,
  AnonymousSchema_3Values[AnonymousSchema_3BeadsDotList]: AnonymousSchema_3BeadsDotList,
  AnonymousSchema_3Values[AnonymousSchema_3BeadsDotReady]: AnonymousSchema_3BeadsDotReady,
  AnonymousSchema_3Values[AnonymousSchema_3BeadDotGet]: AnonymousSchema_3BeadDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3BeadDotCreate]: AnonymousSchema_3BeadDotCreate,
  AnonymousSchema_3Values[AnonymousSchema_3BeadDotClose]: AnonymousSchema_3BeadDotClose,
  AnonymousSchema_3Values[AnonymousSchema_3BeadDotUpdate]: AnonymousSchema_3BeadDotUpdate,
  AnonymousSchema_3Values[AnonymousSchema_3MailDotList]: AnonymousSchema_3MailDotList,
  AnonymousSchema_3Values[AnonymousSchema_3MailDotGet]: AnonymousSchema_3MailDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3MailDotRead]: AnonymousSchema_3MailDotRead,
  AnonymousSchema_3Values[AnonymousSchema_3MailDotReply]: AnonymousSchema_3MailDotReply,
  AnonymousSchema_3Values[AnonymousSchema_3MailDotSend]: AnonymousSchema_3MailDotSend,
  AnonymousSchema_3Values[AnonymousSchema_3EventsDotList]: AnonymousSchema_3EventsDotList,
  AnonymousSchema_3Values[AnonymousSchema_3RigsDotList]: AnonymousSchema_3RigsDotList,
  AnonymousSchema_3Values[AnonymousSchema_3ConvoysDotList]: AnonymousSchema_3ConvoysDotList,
  AnonymousSchema_3Values[AnonymousSchema_3ConvoyDotGet]: AnonymousSchema_3ConvoyDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3SlingDotRun]: AnonymousSchema_3SlingDotRun,
  AnonymousSchema_3Values[AnonymousSchema_3ServicesDotList]: AnonymousSchema_3ServicesDotList,
  AnonymousSchema_3Values[AnonymousSchema_3ServiceDotGet]: AnonymousSchema_3ServiceDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3ServiceDotRestart]: AnonymousSchema_3ServiceDotRestart,
  AnonymousSchema_3Values[AnonymousSchema_3PacksDotList]: AnonymousSchema_3PacksDotList,
  AnonymousSchema_3Values[AnonymousSchema_3ProvidersDotList]: AnonymousSchema_3ProvidersDotList,
  AnonymousSchema_3Values[AnonymousSchema_3ProviderDotGet]: AnonymousSchema_3ProviderDotGet,
  AnonymousSchema_3Values[AnonymousSchema_3AgentDotSuspend]: AnonymousSchema_3AgentDotSuspend,
  AnonymousSchema_3Values[AnonymousSchema_3AgentDotResume]: AnonymousSchema_3AgentDotResume,
  AnonymousSchema_3Values[AnonymousSchema_3RigDotSuspend]: AnonymousSchema_3RigDotSuspend,
  AnonymousSchema_3Values[AnonymousSchema_3RigDotResume]: AnonymousSchema_3RigDotResume,
  AnonymousSchema_3Values[AnonymousSchema_3RigDotRestart]: AnonymousSchema_3RigDotRestart,
  AnonymousSchema_3Values[AnonymousSchema_3SessionDotKill]: AnonymousSchema_3SessionDotKill,
  AnonymousSchema_3Values[AnonymousSchema_3SessionDotPending]: AnonymousSchema_3SessionDotPending,
  AnonymousSchema_3Values[AnonymousSchema_3SessionDotSubmit]: AnonymousSchema_3SessionDotSubmit,
  AnonymousSchema_3Values[AnonymousSchema_3SessionDotTranscript]: AnonymousSchema_3SessionDotTranscript,
  AnonymousSchema_3Values[AnonymousSchema_3SubscriptionDotStart]: AnonymousSchema_3SubscriptionDotStart,
  AnonymousSchema_3Values[AnonymousSchema_3SubscriptionDotStop]: AnonymousSchema_3SubscriptionDotStop,
}

 
func (op *AnonymousSchema_3) UnmarshalJSON(raw []byte) error {
  var v any
  if err := json.Unmarshal(raw, &v); err != nil {
  return err
  }
  *op = ValuesToAnonymousSchema_3[v]
  return nil
}

func (op AnonymousSchema_3) MarshalJSON() ([]byte, error) {
  return json.Marshal(op.Value())
}