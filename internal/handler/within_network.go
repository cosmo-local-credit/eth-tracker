package handler

import "context"

// busyContracts is the set of token contracts that emit a Transfer event on
// virtually every block on Celo. There are two reasons a token lands here:
//
//  1. Celo gas-abstraction: users can pay transaction fees with ERC-20 tokens
//     instead of CELO. Every such transaction produces a Transfer from the
//     payer to the fee-handler contract, completely unrelated to the actual
//     business logic we track.
//  2. Popularity: tokens like USDT or USDGLO are used so broadly that most
//     blocks contain at least one of their transfers that has nothing to do
//     with our network.
//
// When a contract is listed here the within-network filter is applied: we only
// forward a log when at least one participant (sender or receiver) is a known
// address in the bootstrapped cache.
//
// NOTE: Some of these tokens are also registered in our GE registry (they will
// be present in the bootstrapped cache). Even so, the participant check still
// applies — we don't want every gas-fee transfer for cUSD just because cUSD is
// a known token.
var busyContracts = map[string]bool{
	// cUSD — Celo Dollar (Celo native stablecoin, also a gas-abstraction token)
	"0x765DE816845861e75A25fCA122bb6898B8B1282a": true,
	// USDT — Tether USD (bridged, very high transfer volume)
	"0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e": true,
	// cKES — Celo Kenyan Shilling
	"0x456a3D042C0DbD3db53D5489e98dFb038553B0d0": true,
	// USDC — USD Coin (bridged)
	"0xcebA9300f2b948710d2653dD7B07f33A8B32118C": true,
	// USDGLO — Glo Dollar
	"0x4F604735c1cF31399C6E711D5962b2B3E0225AD3": true,
	// cEUR — Celo Euro
	"0xD8763CBa276a3738E6DE85b4b3bF5FDed6D6cA73": true,
	// cREAL — Celo Brazilian Real
	"0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787": true,
	// eXOF — Eco CFA Franc
	"0x73F93dcc49cB8A239e2032663e9475dd5ef29A08": true,
	// PUSO — Philippine Peso (Celo)
	"0x105d4A9306D2E55a71d2Eb95B81553AE1dC20d7B": true,
	// cCOP — Celo Colombian Peso
	"0x8A567e2aE79CA692Bd748aB832081C45de4041eA": true,
	// cGHS — Celo Ghanaian Cedi
	"0xfAeA5F3404bbA20D3cc2f8C4B0A888F55a3c7313": true,
	// cGBP — Celo British Pound
	"0xCCF663b1fF11028f0b19058d0f7B674004a40746": true,
	// cZAR — Celo South African Rand
	"0x4c35853A3B4e647fD266f4de678dCc8fEC410BF6": true,
	// cCAD — Celo Canadian Dollar
	"0xff4Ab19391af240c311c54200a492233052B6325": true,
	// cAUD — Celo Australian Dollar
	"0x7175504C455076F15c04A2F90a8e352281F492F9": true,
	// cCHF — Celo Swiss Franc
	"0xb55a79F398E759E43C95b979163f30eC87Ee131D": true,
	// cNGN — Celo Nigerian Naira
	"0xE2702Bd97ee33c88c8f6f92DA3B733608aa76F71": true,
	// cJPY — Celo Japanese Yen
	"0xc45eCF20f3CD864B32D9794d6f76814aE8892e20": true,
	// axlREGEN — Axelar-bridged REGEN token
	"0x2E6C05f1f7D1f4Eb9A088bf12257f1647682b754": true,
}

// checkWithinNetwork decides whether a token transfer log should be forwarded
// to downstream consumers. It is called for every matched log, so the hot path
// must stay allocation-free.
//
// Decision rules, evaluated in order:
//
//  1. Non-busy contract → always forward.
//     Contracts not in busyContracts are either swap pools, internal protocol
//     contracts, or other low-frequency tokens. All of them are relevant; we
//     never discard their logs. This covers every address in the bootstrapped
//     cache that is not also a busy token.
//
//  2. Busy contract → forward only when a participant is in our network.
//     A busy token is listed in busyContracts because it emits transfers
//     constantly (gas-fee payments, popular stablecoins). We only care about
//     transfers where at least one of the parties — sender (from) or receiver
//     (to) — is a known address in the bootstrapped cache. The token contract
//     itself does not need to be in the cache; our registered users may hold
//     or use a busy token that isn't in our registry.
//
// The bootstrapped cache contains: all GE-registry smart contracts (tokens,
// pools, index contracts), custodial system accounts, and all on-chain user
// EOAs enumerated from the account index. EOAs and smart contracts are stored
// identically — distinction is not needed because the lookup is the same for
// both.
func (hc *HandlerContainer) checkWithinNetwork(ctx context.Context, contractAddress string, from string, to string) (bool, error) {
	// Rule 1: non-busy contracts are always forwarded.
	if !busyContracts[contractAddress] {
		return true, nil
	}

	// Rule 2: busy token — only forward when sender or receiver is known.
	// We intentionally do NOT check whether contractAddress itself is in the
	// cache. A busy token present in the registry would still generate
	// irrelevant gas-fee transfers; a busy token absent from the registry can
	// still be used by our registered users. In both cases the participant
	// check is the correct gate.
	return hc.cache.ExistsAny(ctx, from, to)
}
