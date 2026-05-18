# dbn-go — Go library for DBN and Databento APIs

**Go bindings** for [Databento](https://databento.com)'s [DBN (Databento Binary Encoding)](https://databento.com/docs/knowledge-base/new-users/dbn-encoding), the **Historical API**, and the **Live API**.

This fork (`github.com/WT-In/dbn-go`) is **library-only**: no bundled CLIs or docs site. It is forked from [NimbleMarkets/dbn-go](https://github.com/NimbleMarkets/dbn-go) under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0); see [LICENSE.txt](./LICENSE.txt).

**NOTE:** Not affiliated with Databento. Historical and Live APIs may incur billing; you are responsible for API usage and charges.

## Contents

- [Library usage](#library-usage)
- [Reading DBN files](#reading-dbn-files)
- [Reading JSON files](#reading-json-files)
- [Historical API](#historical-api)
- [Live API](#live-api)

## Library usage

Import the root package for DBN decoding, plus optional subpackages for HTTP APIs:

```go
import (
    dbn "github.com/WT-In/dbn-go"
    dbn_hist "github.com/WT-In/dbn-go/hist"
    dbn_live "github.com/WT-In/dbn-go/live"
)
```

Most types live in [`structs.go`](structs.go) and enums in [`consts.go`](consts.go). Message types include `Mbp0Msg`, `MboMsg`, `Mbp1Msg`, `Cmbp1Msg`, `Mbp10Msg`, `OhlcvMsg`, `ImbalanceMsg`, `SymbolMappingMsg`, `ErrorMsg`, `SystemMsg`, `StatMsg`, `StatusMsg`, `InstrumentDefMsg`, and related versioned structs.

## Reading DBN files

For a homogeneous slice of records, use [`ReadDBNToSlice`](https://pkg.go.dev/github.com/WT-In/dbn-go#ReadDBNToSlice). [`MakeCompressedReader`](https://pkg.go.dev/github.com/WT-In/dbn-go#MakeCompressedReader) wraps an `io.Reader` for `.zst` / `.zstd` inputs.

```go
file, closer, err := dbn.MakeCompressedReader("ohlcv-1s.dbn.zstd", false)
if err != nil {
    return err
}
defer closer.Close()
records, metadata, err := dbn.ReadDBNToSlice[dbn.OhlcvMsg](file)
```

For streaming, use [`DbnScanner`](https://pkg.go.dev/github.com/WT-In/dbn-go#DbnScanner) and optionally the [`Visitor`](https://pkg.go.dev/github.com/WT-In/dbn-go#Visitor) interface:

```go
dbnFile, err := os.Open("ohlcv-1s.dbn")
if err != nil {
    return err
}
defer dbnFile.Close()

dbnScanner := dbn.NewDbnScanner(dbnFile)
metadata, err := dbnScanner.Metadata()
if err != nil {
    return fmt.Errorf("scanner failed to read metadata: %w", err)
}
for dbnScanner.Next() {
    header, err := dbnScanner.GetLastHeader()
    if err != nil {
        return err
    }
    fmt.Printf("rtype: %s  ts: %d\n", header.RType, header.TsEvent)
    _, err = dbn.DbnScannerDecode[dbn.OhlcvMsg](dbnScanner)
    if err != nil {
        return err
    }
    // or: err = dbnScanner.Visit(visitor)
}
if err := dbnScanner.Error(); err != nil && err != io.EOF {
    return fmt.Errorf("scanner error: %w", err)
}
```

## Reading JSON files

Use [`ReadJsonToSlice`](https://pkg.go.dev/github.com/WT-In/dbn-go#ReadJsonToSlice) or [`JsonScanner`](https://pkg.go.dev/github.com/WT-In/dbn-go#JsonScanner) for DBN-shaped JSON streams. Structs use [`valyala/fastjson`](https://github.com/valyala/fastjson) for decoding, not only `encoding/json`.

```go
jsonFile, err := os.Open("ohlcv-1s.dbn.json")
if err != nil {
    return err
}
defer jsonFile.Close()
records, err := dbn.ReadJsonToSlice[dbn.OhlcvMsg](jsonFile)
```

## Historical API

Functions in [`hist`](https://pkg.go.dev/github.com/WT-In/dbn-go/hist) map to [Databento Historical API](https://databento.com/docs/api-reference-historical) endpoints. Each takes an API key string plus parameters and returns typed values and `error`.

```go
apiKey := os.Getenv("DATABENTO_API_KEY")
schemas, err := dbn_hist.ListSchemas(apiKey, "EQUS.MINI")
```

Integration-style tests use `-tags=integration` and `DATABENTO_API_KEY`; see [`hist/hist_integration_test.go`](hist/hist_integration_test.go).

## Live API

[`live`](https://pkg.go.dev/github.com/WT-In/dbn-go/live) implements the TCP gateway protocol for [Databento Live](https://databento.com/docs/api-reference-live):

1. [`NewLiveClient`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#NewLiveClient) with [`LiveConfig`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveConfig)
2. [`Authenticate`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveClient.Authenticate)
3. [`Subscribe`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveClient.Subscribe)
4. [`Start`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveClient.Start)
5. Read with [`GetDbnScanner`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveClient.GetDbnScanner) or [`GetJsonScanner`](https://pkg.go.dev/github.com/WT-In/dbn-go/live#LiveClient.GetJsonScanner)

## Development

```bash
go test ./...
task test          # same as above via Taskfile
go test -tags=integration ./hist   # optional; needs DATABENTO_API_KEY
```

## License

Released under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0); see [LICENSE.txt](./LICENSE.txt).

Portions adapted from [`databento/dbn`](https://github.com/databento/dbn) and [`databento/databento-rs`](https://github.com/databento/databento-rs) under the same license.

Copyright (c) 2024–2026 [Neomantra Corp](https://www.neomantra.com).

Upstream tooling and documentation originated with [Nimble.Markets](https://nimble.markets).
