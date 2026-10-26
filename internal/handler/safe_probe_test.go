package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/grassrootseconomics/ethutils"
	"github.com/lmittmann/w3"
	"github.com/stretchr/testify/require"
)

type probeChain struct {
	chain.Chain
	provider *ethutils.Provider
}

func (c probeChain) Provider() *ethutils.Provider { return c.provider }

func TestSafeProbeNeedsOnlyOwnersAndRetriesRPCFailures(t *testing.T) {
	for _, outage := range []bool{false, true} {
		t.Run(fmt.Sprint("rpc failure=", outage), func(t *testing.T) {
			typ, err := abi.NewType("address[]", "", nil)
			require.NoError(t, err)
			encoded, err := (abi.Arguments{{Type: typ}}).Pack([]common.Address{common.HexToAddress(testOwner)})
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     any               `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
				switch request.Method {
				case "eth_getCode":
					response["result"] = "0x6080"
				case "eth_call":
					var call map[string]string
					require.NoError(t, json.Unmarshal(request.Params[0], &call))
					input := call["input"]
					if input == "" {
						input = call["data"]
					}
					require.Equal(t, fmt.Sprintf("0x%x", safeGetOwnersFunc.Selector), input, "VERSION and threshold must not gate recognition")
					if outage {
						response["error"] = map[string]any{"code": -32000, "message": "missing trie node"}
					} else {
						response["result"] = fmt.Sprintf("0x%x", encoded)
					}
				default:
					t.Errorf("unexpected method %s", request.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client, err := w3.Dial(server.URL)
			require.NoError(t, err)
			defer func() { _ = client.Close() }()
			compatible, err := NewSafeProber(probeChain{provider: &ethutils.Provider{Client: client}})(context.Background(), common.HexToAddress(testSafe))
			if outage {
				require.Error(t, err)
				require.False(t, compatible)
			} else {
				require.NoError(t, err)
				require.True(t, compatible)
			}
		})
	}
}
