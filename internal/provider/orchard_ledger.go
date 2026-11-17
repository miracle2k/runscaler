package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type orchardRecord struct {
	Name            string    `json:"name"`
	VMName          string    `json:"vm_name"`
	UID             string    `json:"uid,omitempty"`
	VM              orchardVM `json:"vm"`
	CreatedAt       time.Time `json:"created_at"`
	JITRequested    bool      `json:"jit_requested,omitempty"`
	RunnerID        int       `json:"runner_id,omitempty"`
	JITRemoved      bool      `json:"jit_removed,omitempty"`
	LaunchAttempted bool      `json:"launch_attempted,omitempty"`
	InstanceRemoved bool      `json:"instance_removed,omitempty"`
	CreateUncertain bool      `json:"create_uncertain,omitempty"`
}

type orchardLedgerData struct {
	Version       int                      `json:"version"`
	OwnerID       string                   `json:"owner_id"`
	Identity      string                   `json:"identity"`
	ControllerURL string                   `json:"controller_url"`
	Records       map[string]orchardRecord `json:"records"`
}

// One ledger belongs to one scale set. Atomic replacement plus directory
// fsync makes each intent durable before its corresponding remote action.
// The manager's process lock excludes simultaneous writers.
type orchardLedger struct {
	mu          sync.Mutex
	path        string
	data        orchardLedgerData
	ownerPrefix string // immutable after open; data is replaced by each transaction
}

func openOrchardLedger(dir, identity, controllerURL string) (*orchardLedger, error) {
	if dir == "" {
		return nil, errors.New("orchard state directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.New("create Orchard state directory failed")
	}
	hash := sha256.Sum256([]byte(identity))
	path := filepath.Join(dir, "orchard-"+hex.EncodeToString(hash[:12])+".json")
	l := &orchardLedger{path: path}
	endpoint, err := url.Parse(controllerURL)
	if err != nil {
		return nil, errors.New("invalid Orchard ledger controller URL")
	}
	endpoint.Scheme = strings.ToLower(endpoint.Scheme)
	endpoint.Host = strings.ToLower(endpoint.Host)
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	controllerURL = endpoint.String()
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if json.Unmarshal(data, &l.data) != nil || l.data.Version != 1 || l.data.Identity != identity || l.data.Records == nil {
			return nil, errors.New("orchard ledger is invalid; restore its saved state before recovery")
		}
		if _, err := uuid.Parse(l.data.OwnerID); err != nil {
			return nil, errors.New("orchard ledger owner is invalid")
		}
		l.ownerPrefix = orchardOwnerPrefix(l.data.OwnerID)
		for id, r := range l.data.Records {
			if id != r.VMName || r.Name == "" || r.VM.Name != id || id != l.prefix()+r.Name || r.VM.Password != "" {
				return nil, errors.New("orchard ledger has an invalid ownership record")
			}
		}
		if l.data.ControllerURL != controllerURL {
			if len(l.data.Records) != 0 {
				return nil, errors.New("orchard controller URL changed with owned records; restore the original endpoint before recovery")
			}
			l.data.ControllerURL = controllerURL
			if err := l.save(l.data); err != nil {
				return nil, err
			}
		}
	case os.IsNotExist(err):
		l.data = orchardLedgerData{Version: 1, OwnerID: uuid.NewString(), Identity: identity, ControllerURL: controllerURL, Records: map[string]orchardRecord{}}
		l.ownerPrefix = orchardOwnerPrefix(l.data.OwnerID)
		if err := l.save(l.data); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("read Orchard ledger failed")
	}
	return l, nil
}

func (l *orchardLedger) prefix() string {
	return l.ownerPrefix
}

func orchardOwnerPrefix(ownerID string) string {
	// A full runner UUID plus this owner prefix fits Orchard's next 63-byte
	// name limit. The ledger also checks the full controller-issued VM UID.
	return "rs-" + strings.ReplaceAll(ownerID, "-", "")[:16] + "-"
}

func (l *orchardLedger) get(id string) (orchardRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.data.Records[id]
	return r, ok
}

func (l *orchardLedger) records() []orchardRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]orchardRecord, 0, len(l.data.Records))
	for _, r := range l.data.Records {
		result = append(result, r)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VMName < result[j].VMName })
	return result
}

func (l *orchardLedger) change(id string, change func(*orchardRecord) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	next := l.data
	next.Records = make(map[string]orchardRecord, len(l.data.Records)+1)
	for key, value := range l.data.Records {
		next.Records[key] = value
	}
	r := next.Records[id]
	if err := change(&r); err != nil {
		return err
	}
	if r.InstanceRemoved && !r.CreateUncertain && (!r.JITRequested || r.JITRemoved) {
		delete(next.Records, id)
	} else {
		next.Records[id] = r
	}
	if err := l.save(next); err != nil {
		return err
	}
	l.data = next
	return nil
}

func (l *orchardLedger) save(data orchardLedgerData) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return errors.New("encode Orchard ledger failed")
	}
	dir := filepath.Dir(l.path)
	file, err := os.CreateTemp(dir, ".orchard-intent-*")
	if err != nil {
		return errors.New("create Orchard ledger transaction failed")
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(append(raw, '\n'))
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp, l.path)
	}
	if err != nil {
		return errors.New("save Orchard ledger transaction failed")
	}
	directory, err := os.Open(dir)
	if err != nil {
		return errors.New("open Orchard state directory for sync failed")
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("sync Orchard state directory failed")
	}
	return nil
}
