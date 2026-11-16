// Package facts 实现冻结的 event_id 派生规则（S2 LLD §3.4）。
package facts

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Derive 返回 sha256(tenant|message_id|fact_type|endpoint_id|version) 的 hex，
// 取前 32 个字符。endpointID == nil 表示消息级事实。
func Derive(tenant, messageID, factType string, endpointID *string, version int) string {
	ep := ""
	if endpointID != nil {
		ep = *endpointID
	}
	raw := tenant + "|" + messageID + "|" + factType + "|" + ep + "|" + strconv.Itoa(version)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:32]
}
