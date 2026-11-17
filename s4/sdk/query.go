package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrQueryUnavailable 表示查询 API 未配置或探测失败。
	ErrQueryUnavailable = errors.New("sdk: client query API unavailable")
	// ErrQueryNotFound 表示消息不存在或不属于当前身份（服务端返回 404）。
	ErrQueryNotFound = errors.New("sdk: message not found")
)

// QueryClient 是端侧只读查询客户端（S4 LLD §8.1）。
type QueryClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

func NewQueryClient(baseURL, token string, timeout time.Duration) *QueryClient {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &QueryClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		hc:      &http.Client{Timeout: timeout},
	}
}

func (q *QueryClient) do(ctx context.Context, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	return q.hc.Do(req)
}

// Probe 发送一次空批量查询：2xx 视为查询 API 可用（LLD §11 硬规则）。
func (q *QueryClient) Probe(ctx context.Context) error {
	if q == nil || q.baseURL == "" {
		return ErrQueryUnavailable
	}
	resp, err := q.do(ctx, q.baseURL+"/v1/client/messages?ids=")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%w: status %d", ErrQueryUnavailable, resp.StatusCode)
	}
	return nil
}

// Get 查询单条消息。
func (q *QueryClient) Get(ctx context.Context, id string) (ClientMessageView, error) {
	if q == nil || q.baseURL == "" {
		return ClientMessageView{}, ErrQueryUnavailable
	}
	target := q.baseURL + "/v1/client/messages/" + url.PathEscape(id)
	resp, err := q.do(ctx, target)
	if err != nil {
		return ClientMessageView{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ClientMessageView{}, ErrQueryNotFound
	}
	if resp.StatusCode/100 != 2 {
		return ClientMessageView{}, fmt.Errorf("sdk: query status %d", resp.StatusCode)
	}
	var v ClientMessageView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return ClientMessageView{}, err
	}
	return v, nil
}

// maxQueryIDs 与服务端 GET /v1/client/messages 的单次 ids 上限一致：
// 服务端超出部分会静默截断，SDK 若不分组会让其余 pending 永远补不到终态。
const maxQueryIDs = 100

// GetBatch 批量查询（重连后补齐 pending）。按服务端上限分组，避免截断。
func (q *QueryClient) GetBatch(ctx context.Context, ids []string) ([]ClientMessageView, error) {
	if q == nil || q.baseURL == "" {
		return nil, ErrQueryUnavailable
	}
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]ClientMessageView, 0, len(ids))
	for start := 0; start < len(ids); start += maxQueryIDs {
		end := start + maxQueryIDs
		if end > len(ids) {
			end = len(ids)
		}
		views, err := q.getBatchChunk(ctx, ids[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, views...)
	}
	return out, nil
}

func (q *QueryClient) getBatchChunk(ctx context.Context, ids []string) ([]ClientMessageView, error) {
	vals := url.Values{}
	vals.Set("ids", strings.Join(ids, ","))
	target := q.baseURL + "/v1/client/messages?" + vals.Encode()
	resp, err := q.do(ctx, target)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("sdk: batch query status %d", resp.StatusCode)
	}
	var out struct {
		Messages []ClientMessageView `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}
