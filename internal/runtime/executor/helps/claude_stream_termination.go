package helps

import (
	"bytes"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	gjson "github.com/MoeForge/moego/ojson"
)

// ClaudeStreamTermination retains only sanitized metadata for one upstream stream.
type ClaudeStreamTermination struct {
	messageID, stopReason                                             string
	messageStop                                                       bool
	textBlocks, thinkingBlocks, toolUseBlocks, otherBlocks, textBytes uint64
	outputTokens                                                      uint64
}

func (s *ClaudeStreamTermination) Observe(line []byte) {
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(line[len("data:"):])
	if !gjson.ValidBytes(payload) {
		return
	}
	root := gjson.ParseBytes(payload)
	usage := root.Get("usage.output_tokens")
	switch root.Get("type").String() {
	case "message_start":
		id := root.Get("message.id")
		s.messageID = ""
		if id.Exists() && id.Type != gjson.Null {
			s.messageID = "invalid"
			if id.Type == gjson.String {
				value := id.String()
				valid := value == "" || (len(value) > 4 && len(value) <= 96 && strings.HasPrefix(value, "msg_"))
				for _, c := range value {
					if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
						valid = false
					}
				}
				if valid {
					s.messageID = value
				}
			}
		}
		usage = root.Get("message.usage.output_tokens")
	case "message_delta":
		reason := root.Get("delta.stop_reason")
		switch reason.String() {
		case "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal", "model_context_window_exceeded":
			s.stopReason = reason.String()
		default:
			if reason.Exists() && reason.Type != gjson.Null {
				s.stopReason = "unknown"
			}
		}
	case "message_stop":
		s.messageStop = true
	case "content_block_start":
		switch root.Get("content_block.type").String() {
		case "text":
			s.textBlocks++
			if text := root.Get("content_block.text"); text.Type == gjson.String {
				s.textBytes += uint64(len(text.String()))
			}
		case "thinking":
			s.thinkingBlocks++
		case "tool_use":
			s.toolUseBlocks++
		default:
			s.otherBlocks++
		}
	case "content_block_delta":
		if root.Get("delta.type").String() == "text_delta" {
			if text := root.Get("delta.text"); text.Type == gjson.String {
				s.textBytes += uint64(len(text.String()))
			}
		}
	}
	if usage.Type == gjson.Number {
		if n, err := strconv.ParseUint(usage.Raw, 10, 64); err == nil {
			s.outputTokens = n
		}
	}
}

func (s *ClaudeStreamTermination) Log() {
	reason := s.stopReason
	if reason == "" {
		reason = "missing"
	}
	log.WithFields(log.Fields{
		"message_id": s.messageID, "stop_reason": reason, "message_stop": s.messageStop,
		"text_blocks": s.textBlocks, "thinking_blocks": s.thinkingBlocks,
		"tool_use_blocks": s.toolUseBlocks, "other_blocks": s.otherBlocks,
		"text_bytes": s.textBytes, "output_tokens": s.outputTokens,
	}).Info("claude_stream_termination")
}
