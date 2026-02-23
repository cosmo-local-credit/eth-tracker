# eth-tracker

![GitHub Tag](https://img.shields.io/github/v/tag/cosmo-local-credit/eth-tracker)

A fast and lightweight tracker designed to monitor EVM blockchains for live and
historical transaction events, including reverted transactions. It filters these
events and publishes them to NATS for further processing.

It applies deduplication at the NATS level, making it safe to run in a
distributed fashion.

Note: To run it against an L2/EVM chain, you will need to manually add a replace
directive in the `go.mod` file pointing to the EVM chain's `*geth` compatible
source code. This will allow the tracker to process transaction types other than
Ethereum's `0x0, 0x1 and 0x2`.

### Celo L2

We maintain a CEL2 compatible tracker (source and container image) on the `cel2`
branch.

## Getting Started

A `Makefile` is also provided to build the required binaries to run eth-tracker.

### Cache Bootstrap

The tracker keeps a persistent address cache in BoltDB and loads it into memory
on startup. This makes restarts fast while preserving strict in-memory lookup
performance during block processing.

Default mode (no watchlist):

- On warm start, cached addresses are loaded immediately and chain enumeration
    runs in the background to catch up any address changes that happened while
    the tracker was offline.
- On cold start, a full on-chain enumeration is performed to seed the cache.

Watchlist mode (`bootstrap.watchlist` configured with non-empty addresses):

- GE registry bootstrap is skipped.
- The cache is reconciled to the watchlist set.
- `bootstrap.blacklist` is applied last and removes any matching addresses.

At runtime, the cache is kept up-to-date from index `AddressAdded` and
`AddressRemoved` events.

### Prerequisites

- Git
- Docker
- NATS server
- Access to an RPC node, archive preffered

See [docker-compose.yaml](dev/docker-compose.yaml) for an example on how to run
and deploy a single instance.

### 1. Build the Docker image

We provide pre-built images for `linux/amd64`. See the packages tab on Github.

If you are on any other platform:

```bash
git clone https://github.com/cosmo-local-credit/eth-tracker.git
cd eth-tracker
docker buildx build --build-arg BUILD=$(git rev-parse --short HEAD) --tag eth-tracker:$(git rev-parse --short HEAD) --tag eth-tracker:latest .
docker images
```

### 2. Run NATS

For an example, see `dev/docker-compose.yaml`.

### 3. Update config values

See `.env.example` on how to override default values defined in `config.toml`
using env variables. Alternatively, mount your own config.toml either during
build time or Docker runtime.

```bash
# Override only specific config values
nano .env.example
mv .env.example .env
```

Refer to [`config.toml`](config.toml) to understand different config value
settings.

### 4. Run the tracker

```bash
cd dev
docker compose up
```

## Processing NATS messages

### JSON structure

```js
{
    "block": Number,
    "contractAddress": String,
    "success": Boolean,
    "timetamp" Number,
    "transactionHash": String,
    "transactionType": String,
    "payload": Object,
    "index": Number,
}
```

### Monitoring with NATS CLI

Install NATS CLI from
[here](https://github.com/nats-io/natscli?tab=readme-ov-file#installation).

```bash
nats subscribe "TRACKER.*"
```

### DB File

A `tracker_db` file is created on the first run. This keeps track of all blocks
missed by the processor to attempt a retry later on. This file should not be
deleted if you want to maintain resume support for historical tracking across
restarts.

#### DB File Compaction

Once the catchup drain completes and normal operation resumes, stop the tracker
and compact the database to reclaim the disk space and reduce the mmap footprint:

```bash
go install go.etcd.io/bbolt/cmd/bbolt@latest

bbolt compact -o db/tracker_db_compacted db/tracker_db

ls -lh db/tracker_db db/tracker_db_compacted

mv db/tracker_db db/tracker_db_backup
mv db/tracker_db_compacted db/tracker_db
```

After restarting, RSS (From memory mapped bbolt file) will drop to reflect the compacted file size.

## License

[AGPL-3.0](LICENSE).
