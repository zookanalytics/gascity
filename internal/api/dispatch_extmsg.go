package api

import "context"

func init() {
	RegisterAction("extmsg.inbound", ActionDef{
		Description:       "Process inbound external message",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgInboundRequest) (any, error) {
		return s.processExtMsgInbound(context.Background(), payload)
	})

	RegisterAction("extmsg.outbound", ActionDef{
		Description:       "Process outbound external message",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgOutboundRequest) (any, error) {
		return s.processExtMsgOutbound(context.Background(), payload)
	})

	RegisterAction("extmsg.bindings.list", ActionDef{
		Description:       "List external message bindings",
		RequiresCityScope: true,
	}, func(s *Server, payload socketExtMsgBindingsPayload) (any, error) {
		return s.listExtMsgBindings(context.Background(), payload.SessionID)
	})

	RegisterAction("extmsg.bind", ActionDef{
		Description:       "Bind external message channel",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgBindRequest) (any, error) {
		return s.processExtMsgBind(context.Background(), payload)
	})

	RegisterAction("extmsg.unbind", ActionDef{
		Description:       "Unbind external message channel",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgUnbindRequest) (any, error) {
		return s.processExtMsgUnbind(context.Background(), payload)
	})

	RegisterAction("extmsg.groups.lookup", ActionDef{
		Description:       "Look up external message group",
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgGroupLookupRequest) (any, error) {
		return s.lookupExtMsgGroup(context.Background(), payload)
	})

	RegisterAction("extmsg.groups.ensure", ActionDef{
		Description:       "Ensure external message group exists",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgGroupEnsureRequest) (any, error) {
		return s.ensureExtMsgGroup(context.Background(), payload)
	})

	RegisterAction("extmsg.participant.upsert", ActionDef{
		Description:       "Upsert external message participant",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgParticipantUpsertRequest) (any, error) {
		return s.upsertExtMsgParticipant(context.Background(), payload)
	})

	RegisterAction("extmsg.participant.remove", ActionDef{
		Description:       "Remove external message participant",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgParticipantRemoveRequest) (any, error) {
		return s.removeExtMsgParticipant(context.Background(), payload)
	})

	RegisterAction("extmsg.transcript.list", ActionDef{
		Description:       "List external message transcript",
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgTranscriptListRequest) (any, error) {
		return s.listExtMsgTranscript(context.Background(), payload)
	})

	RegisterAction("extmsg.transcript.ack", ActionDef{
		Description:       "Acknowledge external message transcript",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgTranscriptAckRequest) (any, error) {
		return s.ackExtMsgTranscript(context.Background(), payload)
	})

	RegisterVoidAction("extmsg.adapters.list", ActionDef{
		Description:       "List external message adapters",
		RequiresCityScope: true,
	}, func(s *Server) (any, error) {
		return s.listExtMsgAdapters()
	})

	RegisterAction("extmsg.adapters.register", ActionDef{
		Description:       "Register external message adapter",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgAdapterRegisterRequest) (any, error) {
		return s.registerExtMsgAdapter(payload)
	})

	RegisterAction("extmsg.adapters.unregister", ActionDef{
		Description:       "Unregister external message adapter",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload extmsgAdapterUnregisterRequest) (any, error) {
		return s.unregisterExtMsgAdapter(payload)
	})
}
