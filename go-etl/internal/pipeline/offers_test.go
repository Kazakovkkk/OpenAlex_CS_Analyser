package pipeline

import (
	"errors"
	"testing"
	"time"
)

func TestOffersIssueAndTake(t *testing.T) {
	t.Parallel()
	o := NewOffers(time.Minute)
	off := o.Issue(Offer{PreviousManifest: "a", NewManifest: "b", Database: "openalex", Revision: 7})
	if off.Token == "" {
		t.Fatal("empty token")
	}
	if o.Len() != 1 {
		t.Fatalf("Len = %d, want 1", o.Len())
	}

	got, err := o.Take(off.Token, "openalex", 7)
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if got.NewManifest != "b" {
		t.Fatalf("offer = %+v", got)
	}
	if o.Len() != 0 {
		t.Fatalf("token must be one-shot, Len = %d", o.Len())
	}
	if _, err := o.Take(off.Token, "openalex", 7); !errors.Is(err, ErrRestartOfferExpired) {
		t.Fatalf("second Take = %v, want ErrRestartOfferExpired", err)
	}
}

func TestOffersRejectsChangedConditions(t *testing.T) {
	t.Parallel()
	o := NewOffers(time.Minute)

	a := o.Issue(Offer{Database: "openalex", Revision: 1})
	if _, err := o.Take(a.Token, "other_db", 1); !errors.Is(err, ErrRestartOfferExpired) {
		t.Fatalf("wrong database accepted: %v", err)
	}
	b := o.Issue(Offer{Database: "openalex", Revision: 2})
	if _, err := o.Take(b.Token, "openalex", 3); !errors.Is(err, ErrRestartOfferExpired) {
		t.Fatalf("wrong revision accepted: %v", err)
	}
}

func TestOffersExpire(t *testing.T) {
	t.Parallel()
	o := NewOffers(time.Minute)
	now := time.Now()
	o.now = func() time.Time { return now }
	off := o.Issue(Offer{Database: "openalex", Revision: 1})

	now = now.Add(2 * time.Minute)
	if _, err := o.Take(off.Token, "openalex", 1); !errors.Is(err, ErrRestartOfferExpired) {
		t.Fatalf("expired token accepted: %v", err)
	}
}

func TestSnapshotChangedError(t *testing.T) {
	t.Parallel()
	err := &SnapshotChangedError{Token: "t", PreviousManifest: "old", CurrentManifest: "new"}
	if err.Error() == "" {
		t.Fatal("empty error text")
	}
	var target *SnapshotChangedError
	if !errors.As(error(err), &target) || target.Token != "t" {
		t.Fatal("error does not unwrap to SnapshotChangedError")
	}
}
