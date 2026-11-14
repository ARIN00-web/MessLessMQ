package server

import (
	"fmt"
	"strings"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/security"
)

// CLIENT_ACL is a semicolon-separated list of role:topic:action rules. Topic
// and action accept *; actions are produce, consume, metadata, or group.
func configureClientACL(acl *security.ACLAuthorizer, raw string) error {
	if acl == nil {
		return security.ErrForbidden
	}
	if strings.TrimSpace(raw) == "" {
		return acl.Grant("client", "*", "*")
	}
	for _, rule := range strings.Split(raw, ";") {
		parts := strings.Split(strings.TrimSpace(rule), ":")
		if len(parts) != 3 {
			return fmt.Errorf("invalid CLIENT_ACL rule %q", rule)
		}
		if err := acl.Grant(parts[0], parts[1], parts[2]); err != nil {
			return err
		}
	}
	return nil
}

func requestTopics(request protocol.Request) ([]string, string, error) {
	switch request.APIKey {
	case protocol.APIPRODUCE:
		var produced protocol.ProduceRequest
		var err error
		if request.APIVersion == protocol.ProduceAPIVersion2 {
			v2, e := protocol.DecodeProduceRequestV2(request.Payload)
			produced, err = v2.Request, e
		} else {
			produced, err = protocol.DecodeProduceRequest(request.Payload)
		}
		if err != nil {
			return nil, "", err
		}
		topics := make([]string, 0, len(produced.Batch.Messages))
		for _, message := range produced.Batch.Messages {
			topics = append(topics, message.Topic)
		}
		return topics, "produce", nil
	case protocol.APICONSUME:
		request, err := protocol.DecodeConsumeRequest(request.Payload)
		if err != nil {
			return nil, "", err
		}
		return []string{request.Topic}, "consume", nil
	case protocol.APIMETADATA:
		request, err := protocol.DecodeMetadataRequest(request.Payload)
		if err != nil {
			return nil, "", err
		}
		if request.Topic == "" {
			return []string{"*"}, "metadata", nil
		}
		return []string{request.Topic}, "metadata", nil
	case protocol.APIGROUP:
		request, err := protocol.DecodeGroupRequest(request.Payload)
		if err != nil {
			return nil, "", err
		}
		if request.Operation == protocol.GroupJoin {
			return append([]string(nil), request.Topics...), "group", nil
		}
		if request.Topic == "" {
			return []string{"*"}, "group", nil
		}
		return []string{request.Topic}, "group", nil
	default:
		return []string{"*"}, "admin", nil
	}
}
