package pipeline

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrRestartOfferExpired is returned when a restart token is unknown, used or
// no longer matches the state it was issued for.
var ErrRestartOfferExpired = errors.New("pipeline: restart offer expired or invalid")

// SnapshotChangedError tells the caller that the release changed while loading;
// the load must not publish, and restarting requires the token it carries.
type SnapshotChangedError struct {
	Token            string
	PreviousManifest string
	CurrentManifest  string
}

func (e *SnapshotChangedError) Error() string {
	return fmt.Sprintf("pipeline: snapshot changed during load (published %q, bucket %q)",
		e.PreviousManifest, e.CurrentManifest)
}

// Offer is a one-shot permission to drop the current corpus and reload it.
type Offer struct {
	Token            string
	PreviousManifest string
	NewManifest      string
	Database         string
	Revision         uint64
	JobID            string
	ExpiresAt        time.Time
}

type Offers struct {
	mu    sync.Mutex
	items map[string]Offer
	ttl   time.Duration
	now   func() time.Time
}

func NewOffers(ttl time.Duration) *Offers {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return &Offers{items: make(map[string]Offer, 4), ttl: ttl, now: time.Now}
}

// Issue stores an offer and returns it with a fresh token and expiry.
func (o *Offers) Issue(in Offer) Offer {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		in.Token = fmt.Sprintf("t%d", time.Now().UnixNano())
	} else {
		in.Token = hex.EncodeToString(buf[:])
	}
	in.ExpiresAt = o.now().Add(o.ttl)

	o.mu.Lock()
	o.items[in.Token] = in
	o.mu.Unlock()
	return in
}

// Take consumes a token if it is still valid for this database and revision.
func (o *Offers) Take(token, database string, revision uint64) (Offer, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	off, ok := o.items[token]
	if !ok {
		return Offer{}, ErrRestartOfferExpired
	}
	delete(o.items, token)

	if o.now().After(off.ExpiresAt) || off.Database != database || off.Revision != revision {
		return Offer{}, ErrRestartOfferExpired
	}
	return off, nil
}

func (o *Offers) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.items)
}
