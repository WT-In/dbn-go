// Copyright (c) 2024 Neomantra Corp
//
// NOTE: this incurs billing, handle with care!
//

package main

import (
	"fmt"
	"io"
	"os"
	"time"

	dbn "github.com/WT-In/dbn-go"
	dbn_live "github.com/WT-In/dbn-go/live"
	"github.com/relvacode/iso8601"
	"github.com/spf13/pflag"
)

///////////////////////////////////////////////////////////////////////////////

type Config struct {
	OutFilename string
	ApiKey      string
	Dataset     string
	STypeIn     dbn.SType
	Encoding    dbn.Encoding
	Schemas     []string
	Symbols     []string
	StartTime   time.Time
	Snapshot    bool
	Verbose     bool
}

///////////////////////////////////////////////////////////////////////////////

func main() {
	var err error
	var config Config
	var startTimeArg string
	var showHelp bool

	config.STypeIn = dbn.SType_RawSymbol

	pflag.StringVarP(&config.Dataset, "dataset", "d", "", "Dataset to subscribe to ")
	pflag.StringArrayVarP(&config.Schemas, "schema", "s", []string{}, "Schema to subscribe to (multiple allowed)")
	pflag.StringVarP(&config.ApiKey, "key", "k", "", "Databento API key (or set 'DATABENTO_API_KEY' envvar)")
	pflag.StringVarP(&config.OutFilename, "out", "o", "", "Output filename for DBN stream ('-' for stdout)")
	pflag.VarP(&config.STypeIn, "sin", "i", "Input SType of the symbols. One of instrument_id, id, instr, raw_symbol, raw, smart, continuous, parent, nasdaq, cms")
	pflag.VarP(&config.Encoding, "encoding", "e", "Encoding of the output ('dbn', 'csv', 'json')")
	pflag.StringVarP(&startTimeArg, "start", "t", "", "Start time to request as ISO 8601 format (default: now)")
	pflag.BoolVarP(&config.Snapshot, "snapshot", "n", false, "Enable snapshot on subscription request")
	pflag.BoolVarP(&config.Verbose, "verbose", "v", false, "Verbose logging")
	pflag.BoolVarP(&showHelp, "help", "h", false, "Show help")
	pflag.Parse()

	config.Symbols = pflag.Args()

	if showHelp {
		fmt.Fprintf(os.Stdout, "usage: %s -d <dataset> -s <schema> [opts] symbol1 symbol2 ...\n\n", os.Args[0])
		pflag.PrintDefaults()
		os.Exit(0)
	}

	if startTimeArg != "" {
		config.StartTime, err = iso8601.ParseString(startTimeArg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse --start as ISO 8601 time: %s\n", err.Error())
			os.Exit(1)
		}
	}

	if config.ApiKey == "" {
		config.ApiKey = os.Getenv("DATABENTO_API_KEY")
		requireValOrExit(config.ApiKey, "missing Databento API key, use --key or set DATABENTO_API_KEY envvar\n")
	}

	if len(config.Schemas) == 0 {
		fmt.Fprintf(os.Stderr, "requires at least --schema argument\n")
		os.Exit(1)
	}

	if len(config.Symbols) == 0 {
		fmt.Fprintf(os.Stderr, "requires at least one symbol argument\n")
		os.Exit(1)
	}

	requireValOrExit(config.Dataset, "missing required --dataset")
	requireValOrExit(config.OutFilename, "missing required --out")

	if err := run(config); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
		os.Exit(1)
	}
}

// requireValOrExit exits with an error message if `val` is empty.
func requireValOrExit(val string, errstr string) {
	if val == "" {
		fmt.Fprintf(os.Stderr, "%s\n", errstr)
		os.Exit(1)
	}
}

///////////////////////////////////////////////////////////////////////////////

func run(config Config) error {
	// Create output file before connecting
	outWriter, outCloser, err := dbn.MakeCompressedWriter(config.OutFilename, false)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outCloser()

	// Create and connect LiveClient
	client, err := dbn_live.NewLiveClient(dbn_live.LiveConfig{
		ApiKey:               config.ApiKey,
		Dataset:              config.Dataset,
		Encoding:             config.Encoding,
		SendTsOut:            false,
		VersionUpgradePolicy: dbn.VersionUpgradePolicy_AsIs,
		Verbose:              config.Verbose,
	})
	if err != nil {
		return fmt.Errorf("failed to create LiveClient: %w", err)
	}
	defer client.Stop()

	// Authenticate to server
	if _, err = client.Authenticate(config.ApiKey); err != nil {
		return fmt.Errorf("failed to authenticate LiveClient: %w", err)
	}

	// Subscribe
	for _, schema := range config.Schemas {
		subRequest := dbn_live.SubscriptionRequestMsg{
			Schema:   schema,
			StypeIn:  config.STypeIn,
			Symbols:  config.Symbols,
			Start:    config.StartTime,
			Snapshot: config.Snapshot,
		}
		if err = client.Subscribe(subRequest); err != nil {
			return fmt.Errorf("failed to subscribe LiveClient: %w", err)
		}
	}

	// Start session
	if err = client.Start(); err != nil {
		return fmt.Errorf("failed to start LiveClient: %w", err)
	}

	if config.Encoding == dbn.Encoding_Dbn {
		return followStreamDBN(client, outWriter)
	} else {
		return followStreamJSON(client, outWriter)
	}
}

func followStreamDBN(client *dbn_live.LiveClient, outWriter io.Writer) error {
	// Write metadata to file
	dbnScanner := client.GetDbnScanner()
	if dbnScanner == nil {
		return fmt.Errorf("failed to get DbnScanner from LiveClient")
	}
	metadata, err := dbnScanner.Metadata()
	if err != nil {
		return fmt.Errorf("failed to get metadata from LiveClient: %w", err)
	}
	if err = metadata.Write(outWriter); err != nil {
		return fmt.Errorf("failed to write metadata from LiveClient: %w", err)
	}

	// Follow the DBN stream, writing DBN messages to the file
	for dbnScanner.Next() {
		recordBytes := dbnScanner.GetLastRecord()[:dbnScanner.GetLastSize()]
		_, err := outWriter.Write(recordBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write record: %s\n", err.Error())
			return err
		}
	}
	if err := dbnScanner.Error(); err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "scanner err: %s\n", err.Error())
		return err
	}
	return nil
}

func followStreamJSON(client *dbn_live.LiveClient, outWriter io.Writer) error {
	// Get the JSON scanner
	jsonScanner := client.GetJsonScanner()
	if jsonScanner == nil {
		return fmt.Errorf("failed to get JsonScanner from LiveClient")
	}
	// Follow the JSON stream, writing JSON messages to the file
	for jsonScanner.Next() {
		recordBytes := jsonScanner.GetLastRecord()
		_, err := outWriter.Write(recordBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write record: %s\n", err.Error())
			return err
		}
	}
	if err := jsonScanner.Error(); err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "scanner err: %s\n", err.Error())
		return err
	}
	return nil
}
