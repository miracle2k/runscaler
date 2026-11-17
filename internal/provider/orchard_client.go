package provider

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/coder/websocket"
)

// This is the small, versioned Orchard REST/exec surface needed by this
// provider. Keeping these DTOs local avoids coupling runner builds to the
// Orchard server and its runtime dependencies.
type orchardVM struct {
	Name          string            `json:"name"`
	UID           string            `json:"uid,omitempty"`
	Image         string            `json:"image"`
	CPU           int               `json:"cpu"`
	Memory        int               `json:"memory"`
	Headless      bool              `json:"headless"`
	Username      string            `json:"username,omitempty"`
	Password      string            `json:"password,omitempty"`
	Resources     map[string]uint64 `json:"resources"`
	Labels        map[string]string `json:"labels,omitempty"`
	Status        string            `json:"status,omitempty"`
	RestartPolicy string            `json:"restart_policy"`
}

type orchardStarterRequest struct {
	Action    string `json:"action"`
	Name      string `json:"name,omitempty"`
	RunnerDir string `json:"runner_dir,omitempty"`
	JITConfig string `json:"jit_config,omitempty"`
	MaxBytes  int    `json:"max_bytes,omitempty"`
}

type orchardStarterStatus struct {
	State    string `json:"state"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Log      string `json:"log,omitempty"`
}

type orchardAPI interface {
	Check(context.Context) error
	Create(context.Context, orchardVM) error
	Get(context.Context, string) (orchardVM, error)
	Delete(context.Context, string) error
	Exec(context.Context, string, string, orchardStarterRequest) (orchardStarterStatus, error)
}

func (c *orchardHTTPClient) Check(ctx context.Context) error {
	var info struct {
		Version      string   `json:"version"`
		Capabilities []string `json:"capabilities"`
	}
	if err := c.request(ctx, http.MethodGet, c.endpoint("controller", "info"), nil, &info); err != nil {
		return err
	}
	if info.Version == "" && len(info.Capabilities) == 0 {
		return errors.New("orchard controller info response is invalid")
	}
	return nil
}

type orchardHTTPError struct{ Status int }

func (e *orchardHTTPError) Error() string {
	return fmt.Sprintf("orchard request failed: HTTP %d", e.Status)
}
func orchardNotFound(err error) bool {
	var e *orchardHTTPError
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}
func orchardConflict(err error) bool {
	var e *orchardHTTPError
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

type orchardHTTPClient struct {
	base *url.URL
	http *http.Client
	auth string
}

func newOrchardHTTPClient(rawURL, account, token string) (*orchardHTTPClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return nil, errors.New("invalid Orchard controller URL")
	}
	return &orchardHTTPClient{
		base: u, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		auth: "Basic " + base64.StdEncoding.EncodeToString([]byte(account+":"+token)),
	}, nil
}

func (c *orchardHTTPClient) endpoint(parts ...string) *url.URL {
	return c.base.JoinPath(append([]string{"v1"}, parts...)...)
}

func (c *orchardHTTPClient) request(ctx context.Context, method string, endpoint *url.URL, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return errors.New("encode Orchard request failed")
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return errors.New("build Orchard request failed")
	}
	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return orchardTransportError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &orchardHTTPError{Status: resp.StatusCode}
	}
	if out != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(out); err != nil {
			return errors.New("decode Orchard response failed")
		}
	}
	return nil
}

func orchardTransportError(ctx context.Context, err error) error {
	// HTTP and exec errors can contain complete URLs or response text. Never
	// copy them across the provider's diagnostic boundary.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("orchard transport failed")
}

func (c *orchardHTTPClient) Create(ctx context.Context, vm orchardVM) error {
	return c.request(ctx, http.MethodPost, c.endpoint("vms"), vm, nil)
}
func (c *orchardHTTPClient) Get(ctx context.Context, name string) (orchardVM, error) {
	var vm orchardVM
	err := c.request(ctx, http.MethodGet, c.endpoint("vms", name), nil, &vm)
	return vm, err
}
func (c *orchardHTTPClient) Delete(ctx context.Context, name string) error {
	return c.request(ctx, http.MethodDelete, c.endpoint("vms", name), nil, nil)
}

type orchardExecFrame struct {
	Type string `json:"type"`
	Data []byte `json:"data,omitempty"`
	Exit *struct {
		Code int `json:"code"`
	} `json:"exit,omitempty"`
}

func (c *orchardHTTPClient) Exec(ctx context.Context, name, command string, input orchardStarterRequest) (orchardStarterStatus, error) {
	u := c.endpoint("vms", name, "exec")
	q := u.Query()
	q.Set("command", command)
	q.Set("interactive", "true")
	q.Set("wait", "10")
	u.RawQuery = q.Encode()
	// Command is fixed configuration. Secrets are only sent as stdin frames,
	// never query arguments, environment parameters, or VM startup scripts.
	conn, resp, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPClient: c.http, HTTPHeader: http.Header{"Authorization": []string{c.auth}}})
	if err != nil {
		if resp != nil {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			return orchardStarterStatus{}, &orchardHTTPError{Status: resp.StatusCode}
		}
		return orchardStarterStatus{}, orchardTransportError(ctx, err)
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(512 << 10)
	payload, err := json.Marshal(input)
	if err != nil {
		return orchardStarterStatus{}, errors.New("encode guest starter request failed")
	}
	for _, data := range [][]byte{append(payload, '\n'), nil} {
		frame, _ := json.Marshal(orchardExecFrame{Type: "stdin", Data: data})
		if err = conn.Write(ctx, websocket.MessageText, frame); err != nil {
			return orchardStarterStatus{}, orchardTransportError(ctx, err)
		}
	}
	var output bytes.Buffer
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return orchardStarterStatus{}, orchardTransportError(ctx, err)
		}
		var frame orchardExecFrame
		if json.Unmarshal(data, &frame) != nil {
			return orchardStarterStatus{}, errors.New("invalid Orchard exec frame")
		}
		switch frame.Type {
		case "stdout":
			if output.Len()+len(frame.Data) > 512<<10 {
				return orchardStarterStatus{}, errors.New("guest starter output exceeded limit")
			}
			_, _ = output.Write(frame.Data)
		case "stderr": // Do not expose arbitrary stderr, which can contain secrets.
		case "error":
			return orchardStarterStatus{}, errors.New("orchard guest exec failed")
		case "exit":
			if frame.Exit == nil || frame.Exit.Code != 0 {
				return orchardStarterStatus{}, errors.New("guest starter command failed")
			}
			var status orchardStarterStatus
			if json.Unmarshal(output.Bytes(), &status) != nil || status.State == "" {
				return status, errors.New("invalid guest starter status")
			}
			return status, nil
		}
	}
}
