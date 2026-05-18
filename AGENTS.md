# AGENTS.md — Agent guidelines for dbn-go

Context for AI agents working on **WT-In/dbn-go**, a **library-only** fork of [NimbleMarkets/dbn-go](https://github.com/NimbleMarkets/dbn-go).

## Project overview

**dbn-go** is a Go library for Databento’s DBN format and Historical / Live HTTP+TCP APIs. It is **not** affiliated with Databento.

Capabilities:

- DBN binary and JSON record decoding (root `dbn` package)
- Historical REST client (`hist/`)
- Live gateway client (`live/`)
- Internal build metadata for live auth client string (`internal/version/` — used from `live/gateway.go`)

There is **no** `cmd/` tree in this fork; consume the module with `go get github.com/WT-In/dbn-go`.

## Project structure

```
.
├── *.go                    # Core: structs, consts, scanners, metadata
├── hist/                   # Historical API client
├── live/                   # Live API client
├── internal/
│   └── version/            # Version string for Live client field (non-public API)
├── tests/
│   ├── data/               # Sample DBN files
│   └── stype_matrix/       # Optional symbology probe (go run)
├── Taskfile.yml
├── README.md
├── AGENTS.md
├── CHANGELOG.md
├── LICENSE.txt
└── CODE_OF_CONDUCT.md
```

## Build and test

Prefer plain Go:

```bash
go build ./...
go test ./...
```

Optional Task shortcuts (`Taskfile.yml`):

```bash
task              # default: same as task test
task test         # go test ./...
task test-integration   # integration hist + verbose live (needs credentials where applicable)
task go-tidy
task go-lint
```

Offline-safe tests:

```bash
go test ./...
go test ./live
```

Some `hist` tests need `-tags=integration` and `DATABENTO_API_KEY`; see `hist/hist_integration_test.go`.

## Code style

### File header

Go files should retain the upstream copyright header pattern, e.g.:

```go
// Copyright (c) 2024 Neomantra Corp
```

### Naming (matches upstream)

- Exported types: PascalCase (`DbnScanner`, `OhlcvMsg`)
- Enum-like constants: type prefix with underscores (`Schema_Ohlcv1S`, `SType_InstrumentId`, `RType_Mbp0`)
- Binary decoders: `Fill_Raw`, `Fill_Json` on record types
- Generic constraint: `RecordPtr[T]` for typed decoding

### Package layout

- Root import path: `github.com/WT-In/dbn-go` → package `dbn`
- Historical: `github.com/WT-In/dbn-go/hist` → package `dbn_hist`
- Live: `github.com/WT-In/dbn-go/live` → package `dbn_live`

### Errors

- Sentinels in `errors.go`; wrap with `fmt.Errorf("context: %w", err)` where appropriate.

## Patterns

### Scanner

```go
scanner := dbn.NewDbnScanner(reader)
for scanner.Next() {
    record, err := dbn.DbnScannerDecode[dbn.OhlcvMsg](scanner)
    _ = record
    _ = err
}
```

### Visitor dispatch

Implement `dbn.Visitor` and call `scanner.Visit(visitor)`.

### Compression

```go
file, closer, err := dbn.MakeCompressedReader("file.dbn.zstd", false)
defer closer.Close()
```

## DBN format notes

- Little-endian binary; versions 1–3 with upgrades in scanner paths for mixed-version streams.
- Record header: 16 bytes (`RHeader_Size`).
- Prices: fixed-point scale `FIXED_PRICE_SCALE = 1e9`.

Details: [Databento DBN encoding](https://databento.com/docs/knowledge-base/new-users/dbn-encoding), upstream Rust [databento/dbn](https://github.com/databento/dbn).

## Dependencies (`go.mod`)

- `github.com/valyala/fastjson` — JSON record parsing
- `github.com/klauspost/compress` — zstd for `MakeCompressedReader` / `MakeCompressedWriter`
- `github.com/onsi/ginkgo/v2`, `github.com/onsi/gomega` — tests

## Important notes

1. Not affiliated with Databento; API usage and charges are the user’s responsibility.
2. Typical env var for API examples: `DATABENTO_API_KEY`.
3. Preserve DBN v1 compatibility where upstream did.
4. JSON decoding uses fastjson + hand-written field binding, not only `encoding/json`.

## References

- DBN layout: https://databento.com/docs/knowledge-base/new-users/dbn-encoding
- Rust reference: https://github.com/databento/dbn
- This module (if public on pkg.go.dev): https://pkg.go.dev/github.com/WT-In/dbn-go
