package handler

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	testSafe  = "0x1111111111111111111111111111111111111111"
	testOwner = "0x2222222222222222222222222222222222222222"
)

type fakeCache struct {
	set  map[string]bool
	adds int
}

func newFakeCache() *fakeCache { return &fakeCache{set: map[string]bool{}} }

func (f *fakeCache) Add(_ context.Context, a string) error    { f.set[a] = true; f.adds++; return nil }
func (f *fakeCache) AddBatch(context.Context, []string) error { return nil }
func (f *fakeCache) Remove(context.Context, string) error     { return nil }
func (f *fakeCache) Decrement(context.Context, string) error  { return nil }
func (f *fakeCache) Exists(_ context.Context, a string) (bool, error) {
	return f.set[a], nil
}
func (f *fakeCache) ExistsAny(context.Context, ...string) (bool, error) { return false, nil }
func (f *fakeCache) Size(context.Context) (int64, error)                { return int64(len(f.set)), nil }

func collectEvents(into *[]event.Event) router.Callback {
	return func(_ context.Context, e event.Event) error {
		*into = append(*into, e)
		return nil
	}
}

func word(b []byte) []byte { return common.LeftPadBytes(b, 32) }

func safeLog(topic common.Hash, data []byte) router.LogPayload {
	return router.LogPayload{Log: &types.Log{
		Address:     common.HexToAddress(testSafe),
		BlockNumber: 42,
		Index:       3,
		Topics:      []common.Hash{topic},
		Data:        data,
	}}
}

func TestSafeOwnerEventsEmitOneTypeWithTheChange(t *testing.T) {
	owner := common.HexToAddress(testOwner)
	cases := map[string]struct {
		handler router.LogHandlerFunc
		log     router.LogPayload
		payload map[string]any
	}{
		"added": {HandleSafeAddedOwnerLog(), safeLog(SafeAddedOwnerEvent.Topic0, word(owner.Bytes())),
			map[string]any{"change": "added", "owner": owner.Hex()}},
		"removed": {HandleSafeRemovedOwnerLog(), safeLog(SafeRemovedOwnerEvent.Topic0, word(owner.Bytes())),
			map[string]any{"change": "removed", "owner": owner.Hex()}},
		"threshold": {HandleSafeChangedThresholdLog(), safeLog(SafeChangedThresholdEvent.Topic0, word(big.NewInt(3).Bytes())),
			map[string]any{"change": "threshold", "threshold": "3"}},
		// Safe 1.4.x and 1.5.x index the owner: same topic0, owner in topic 1.
		"added (indexed)": {HandleSafeAddedOwnerLog(), indexedSafeLog(SafeAddedOwnerEvent.Topic0, owner),
			map[string]any{"change": "added", "owner": owner.Hex()}},
		"removed (indexed)": {HandleSafeRemovedOwnerLog(), indexedSafeLog(SafeRemovedOwnerEvent.Topic0, owner),
			map[string]any{"change": "removed", "owner": owner.Hex()}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var got []event.Event
			if err := tc.handler(context.Background(), tc.log, collectEvents(&got)); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("emitted %d events", len(got))
			}
			e := got[0]
			if e.TxType != safeOwnersChangedEventName || !e.Success || e.ContractAddress != common.HexToAddress(testSafe).Hex() ||
				e.Block != 42 || e.Index != 3 {
				t.Fatalf("unexpected event %+v", e)
			}
			for k, v := range tc.payload {
				if e.Payload[k] != v {
					t.Errorf("payload[%s] = %v, want %v", k, e.Payload[k], v)
				}
			}
		})
	}
}

func indexedSafeLog(topic common.Hash, owner common.Address) router.LogPayload {
	lp := safeLog(topic, nil)
	lp.Log.Topics = append(lp.Log.Topics, common.BytesToHash(owner.Bytes()))
	return lp
}

// A log reusing the signature with another layout must not fail the block: the
// change is still reported, without an owner, and consumers re-read the set.
func TestSafeOwnerEventsReportAnUndecodableChangeWithoutFailing(t *testing.T) {
	for name, handler := range map[string]router.LogHandlerFunc{
		"added":   HandleSafeAddedOwnerLog(),
		"removed": HandleSafeRemovedOwnerLog(),
	} {
		t.Run(name, func(t *testing.T) {
			var got []event.Event
			lp := safeLog(SafeAddedOwnerEvent.Topic0, []byte{0x01})
			if err := handler(context.Background(), lp, collectEvents(&got)); err != nil {
				t.Fatalf("an undecodable owner event failed the block: %v", err)
			}
			if len(got) != 1 || got[0].TxType != safeOwnersChangedEventName || got[0].Payload["change"] != name {
				t.Fatalf("unexpected events %+v", got)
			}
			if _, ok := got[0].Payload["owner"]; ok {
				t.Fatalf("owner = %v, want it omitted", got[0].Payload["owner"])
			}
		})
	}
}

func ownershipLog(newOwner common.Address) router.LogPayload {
	return router.LogPayload{Log: &types.Log{
		Address: common.HexToAddress(testSafe),
		Topics: []common.Hash{
			ownershipEvent.Topic0,
			common.BytesToHash(common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes()),
			common.BytesToHash(newOwner.Bytes()),
		},
	}}
}

func TestOwnershipToASafeStartsWatchingIt(t *testing.T) {
	c := newFakeCache()
	hc := New(c, func(context.Context, common.Address) (bool, error) { return true, nil })
	owner := common.HexToAddress(testOwner)
	var got []event.Event
	if err := HandleOwnershipLog(hc)(context.Background(), ownershipLog(owner), collectEvents(&got)); err != nil {
		t.Fatal(err)
	}
	if !c.set[owner.Hex()] || len(got) != 1 {
		t.Fatalf("safe owner not watched or event lost: watched=%v events=%d", c.set, len(got))
	}
	// A replayed block must not inflate the reference count.
	if err := HandleOwnershipLog(hc)(context.Background(), ownershipLog(owner), collectEvents(&got)); err != nil {
		t.Fatal(err)
	}
	if c.adds != 1 {
		t.Fatalf("replay added %d times, want 1", c.adds)
	}
}

func TestOwnershipToAnEOAIsNeverWatched(t *testing.T) {
	c := newFakeCache()
	hc := New(c, func(context.Context, common.Address) (bool, error) { return false, nil })
	var got []event.Event
	if err := HandleOwnershipLog(hc)(context.Background(), ownershipLog(common.HexToAddress(testOwner)), collectEvents(&got)); err != nil {
		t.Fatal(err)
	}
	if len(c.set) != 0 || len(got) != 1 {
		t.Fatalf("EOA was watched or event lost: watched=%v events=%d", c.set, len(got))
	}
}

func TestProbeFailureRetriesTheBlockWithoutLosingTheEvent(t *testing.T) {
	hc := New(newFakeCache(), func(context.Context, common.Address) (bool, error) { return false, errors.New("rpc down") })
	var got []event.Event
	err := HandleOwnershipLog(hc)(context.Background(), ownershipLog(common.HexToAddress(testOwner)), collectEvents(&got))
	if err == nil || len(got) != 0 {
		t.Fatalf("want error and no emission, got err=%v events=%d", err, len(got))
	}
}

func TestSafeEventTopicsMatchKnownValues(t *testing.T) {
	want := map[string]common.Hash{
		"AddedOwner":       common.HexToHash("0x9465fa0c962cc76958e6373a993326400c1c94f8be2fe3a952adfa7f60b2ea26"),
		"RemovedOwner":     common.HexToHash("0xf8d49fc529812e9a7c5c50e69c20f0dccc0db8fa95c98bc58cc9a4f1c1299eaf"),
		"ChangedThreshold": common.HexToHash("0x610f7ff2b304ae8903c3de74c60c6ab1f7d6226b3f52c5161905bb5ad4039c93"),
	}
	got := map[string]common.Hash{
		"AddedOwner": SafeAddedOwnerEvent.Topic0, "RemovedOwner": SafeRemovedOwnerEvent.Topic0,
		"ChangedThreshold": SafeChangedThresholdEvent.Topic0,
	}
	for name, h := range want {
		if got[name] != h {
			t.Errorf("%s topic = %s, want %s", name, got[name], h)
		}
	}
}
