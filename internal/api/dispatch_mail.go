package api

import (
	"github.com/gastownhall/gascity/internal/mail"
)

func init() {
	RegisterAction("mail.list", ActionDef{
		Description:       "List mail messages",
		RequiresCityScope: true,
		SupportsWatch:     true,
	}, func(s *Server, payload socketMailListPayload) (listResponse, error) {
		items, err := s.listMailMessages(payload.Agent, payload.Status, payload.Rig)
		if err != nil {
			return listResponse{}, err
		}
		pp := socketPageParams(payload.Limit, payload.Cursor, 50)
		if !pp.IsPaging {
			total := len(items)
			if pp.Limit < len(items) {
				items = items[:pp.Limit]
			}
			return listResponse{Items: items, Total: total}, nil
		}
		page, total, nextCursor := paginate(items, pp)
		if page == nil {
			page = []mail.Message{}
		}
		return listResponse{Items: page, Total: total, NextCursor: nextCursor}, nil
	})

	RegisterAction("mail.get", ActionDef{
		Description:       "Get a mail message",
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailGetPayload) (mail.Message, error) {
		return s.getMailMessage(payload.ID, payload.Rig)
	})

	RegisterAction("mail.count", ActionDef{
		Description:       "Count mail messages",
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailCountPayload) (map[string]int, error) {
		return s.mailCount(payload.Agent, payload.Rig)
	})

	RegisterAction("mail.thread", ActionDef{
		Description:       "Get mail thread",
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailThreadPayload) (listResponse, error) {
		result, err := s.listMailThread(payload.ID, payload.Rig)
		if err != nil {
			return listResponse{}, err
		}
		return listResponse{Items: result, Total: len(result)}, nil
	})

	RegisterAction("mail.read", ActionDef{
		Description:       "Mark mail as read",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailGetPayload) (map[string]string, error) {
		return s.markMailRead(payload.ID, payload.Rig)
	})

	RegisterAction("mail.mark_unread", ActionDef{
		Description:       "Mark mail as unread",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailGetPayload) (map[string]string, error) {
		return s.markMailUnread(payload.ID, payload.Rig)
	})

	RegisterAction("mail.archive", ActionDef{
		Description:       "Archive a mail message",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailGetPayload) (map[string]string, error) {
		return s.archiveMail(payload.ID, payload.Rig)
	})

	RegisterAction("mail.reply", ActionDef{
		Description:       "Reply to a mail message",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailReplyPayload) (mail.Message, error) {
		return s.replyMail(payload.ID, payload.Rig, mailReplyRequest{
			From:    payload.From,
			Subject: payload.Subject,
			Body:    payload.Body,
		})
	})

	// mail.send uses idempotency — leave on legacy switch.
	RegisterMeta("mail.send", ActionDef{
		Description:       "Send a mail message",
		IsMutation:        true,
		RequiresCityScope: true,
	})

	RegisterAction("mail.delete", ActionDef{
		Description:       "Delete a mail message",
		IsMutation:        true,
		RequiresCityScope: true,
	}, func(s *Server, payload socketMailGetPayload) (map[string]string, error) {
		if err := s.deleteMail(payload.ID, payload.Rig); err != nil {
			return nil, err
		}
		return map[string]string{"status": "deleted"}, nil
	})
}
