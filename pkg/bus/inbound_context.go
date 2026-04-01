package bus

import "strings"

const (
	metadataKeyAccountID      = "account_id"
	metadataKeyGuildID        = "guild_id"
	metadataKeyTeamID         = "team_id"
	metadataKeyReplyToMessage = "reply_to_message_id"
	metadataKeyReplyToSender  = "reply_to_sender_id"
	metadataKeyParentPeerKind = "parent_peer_kind"
	metadataKeyParentPeerID   = "parent_peer_id"
	metadataKeyIsMentioned    = "is_mentioned"
)

// ContextFromLegacyInbound builds a normalized inbound context from the legacy
// top-level fields on InboundMessage. This keeps older producers working while
// new producers migrate to writing Context directly.
func ContextFromLegacyInbound(msg InboundMessage) InboundContext {
	ctx := InboundContext{
		Channel:  strings.TrimSpace(msg.Channel),
		ChatID:   strings.TrimSpace(msg.ChatID),
		ChatType: normalizeKind(msg.Peer.Kind),
		SenderID: firstNonEmpty(
			strings.TrimSpace(msg.SenderID),
			strings.TrimSpace(msg.Sender.CanonicalID),
			strings.TrimSpace(msg.Sender.PlatformID),
		),
		MessageID: strings.TrimSpace(msg.MessageID),
		Raw:       cloneStringMap(msg.Metadata),
	}

	if account := metadataValue(msg.Metadata, metadataKeyAccountID); account != "" {
		ctx.Account = account
	}
	if replyToMsgID := metadataValue(msg.Metadata, metadataKeyReplyToMessage); replyToMsgID != "" {
		ctx.ReplyToMessageID = replyToMsgID
	}
	if replyToSenderID := metadataValue(msg.Metadata, metadataKeyReplyToSender); replyToSenderID != "" {
		ctx.ReplyToSenderID = replyToSenderID
	}
	if isTruthy(metadataValue(msg.Metadata, metadataKeyIsMentioned)) {
		ctx.Mentioned = true
	}

	parentKind := normalizeKind(metadataValue(msg.Metadata, metadataKeyParentPeerKind))
	parentID := metadataValue(msg.Metadata, metadataKeyParentPeerID)
	if parentKind == "topic" && parentID != "" {
		ctx.TopicID = parentID
	}

	switch {
	case metadataValue(msg.Metadata, metadataKeyGuildID) != "":
		ctx.SpaceType = "guild"
		ctx.SpaceID = metadataValue(msg.Metadata, metadataKeyGuildID)
	case metadataValue(msg.Metadata, metadataKeyTeamID) != "":
		ctx.SpaceType = "team"
		ctx.SpaceID = metadataValue(msg.Metadata, metadataKeyTeamID)
	}

	return normalizeInboundContext(ctx)
}

// NormalizeInboundMessage ensures the normalized Context is present and mirrors
// missing legacy fields from it so older consumers continue to work during the
// migration period.
func NormalizeInboundMessage(msg InboundMessage) InboundMessage {
	if msg.Context.isZero() {
		msg.Context = ContextFromLegacyInbound(msg)
	} else {
		msg.Context = normalizeInboundContext(msg.Context)
	}

	if msg.Channel == "" {
		msg.Channel = msg.Context.Channel
	}
	if msg.SenderID == "" {
		msg.SenderID = msg.Context.SenderID
	}
	if msg.ChatID == "" {
		msg.ChatID = msg.Context.ChatID
	}
	if msg.MessageID == "" {
		msg.MessageID = msg.Context.MessageID
	}
	if msg.Peer.Kind == "" {
		msg.Peer = peerFromContext(msg.Context)
	}

	msg.Metadata = mergeLegacyMetadata(msg.Metadata, msg.Context)
	return msg
}

func (ctx InboundContext) isZero() bool {
	return ctx.Channel == "" &&
		ctx.Account == "" &&
		ctx.ChatID == "" &&
		ctx.ChatType == "" &&
		ctx.TopicID == "" &&
		ctx.SpaceID == "" &&
		ctx.SpaceType == "" &&
		ctx.SenderID == "" &&
		ctx.MessageID == "" &&
		!ctx.Mentioned &&
		ctx.ReplyToMessageID == "" &&
		ctx.ReplyToSenderID == "" &&
		len(ctx.ReplyHandles) == 0 &&
		len(ctx.Raw) == 0
}

func normalizeInboundContext(ctx InboundContext) InboundContext {
	ctx.Channel = strings.TrimSpace(ctx.Channel)
	ctx.Account = strings.TrimSpace(ctx.Account)
	ctx.ChatID = strings.TrimSpace(ctx.ChatID)
	ctx.ChatType = normalizeKind(ctx.ChatType)
	ctx.TopicID = strings.TrimSpace(ctx.TopicID)
	ctx.SpaceID = strings.TrimSpace(ctx.SpaceID)
	ctx.SpaceType = normalizeKind(ctx.SpaceType)
	ctx.SenderID = strings.TrimSpace(ctx.SenderID)
	ctx.MessageID = strings.TrimSpace(ctx.MessageID)
	ctx.ReplyToMessageID = strings.TrimSpace(ctx.ReplyToMessageID)
	ctx.ReplyToSenderID = strings.TrimSpace(ctx.ReplyToSenderID)
	ctx.ReplyHandles = cloneStringMap(ctx.ReplyHandles)
	ctx.Raw = cloneStringMap(ctx.Raw)
	return ctx
}

func peerFromContext(ctx InboundContext) Peer {
	kind := normalizeKind(ctx.ChatType)
	if kind == "" {
		return Peer{}
	}

	switch kind {
	case "direct":
		return Peer{
			Kind: "direct",
			ID:   firstNonEmpty(strings.TrimSpace(ctx.SenderID), strings.TrimSpace(ctx.ChatID)),
		}
	case "group", "channel":
		return Peer{
			Kind: kind,
			ID:   strings.TrimSpace(ctx.ChatID),
		}
	default:
		return Peer{
			Kind: kind,
			ID:   strings.TrimSpace(ctx.ChatID),
		}
	}
}

func mergeLegacyMetadata(existing map[string]string, ctx InboundContext) map[string]string {
	merged := cloneStringMap(existing)
	if len(merged) == 0 {
		merged = cloneStringMap(ctx.Raw)
	} else {
		for k, v := range ctx.Raw {
			if _, ok := merged[k]; !ok {
				merged[k] = v
			}
		}
	}

	if ctx.Account != "" {
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyAccountID, ctx.Account)
	}
	if ctx.ReplyToMessageID != "" {
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyReplyToMessage, ctx.ReplyToMessageID)
	}
	if ctx.ReplyToSenderID != "" {
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyReplyToSender, ctx.ReplyToSenderID)
	}
	if ctx.Mentioned {
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyIsMentioned, "true")
	}
	if ctx.TopicID != "" {
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyParentPeerKind, "topic")
		setMissing(merged, metadataKeyParentPeerID, ctx.TopicID)
	}

	switch normalizeKind(ctx.SpaceType) {
	case "guild":
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyGuildID, ctx.SpaceID)
	case "team", "workspace":
		if merged == nil {
			merged = make(map[string]string)
		}
		setMissing(merged, metadataKeyTeamID, ctx.SpaceID)
	}

	if len(merged) == 0 {
		return nil
	}
	return merged
}

func setMissing(dst map[string]string, key, value string) {
	if value == "" {
		return
	}
	if _, ok := dst[key]; !ok {
		dst[key] = value
	}
}

func metadataValue(metadata map[string]string, key string) string {
	if metadata == nil {
		return ""
	}
	return strings.TrimSpace(metadata[key])
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeKind(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func isTruthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}
