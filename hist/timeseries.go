// Copyright (C) 2024 Neomantra Corp

package dbn_hist

import (
	"fmt"
	"io"
	"net/url"
)

// Databento Time Series API:
//  https://databento.com/docs/api-reference-historical/timeseries/timeseries-get-range?historical=http&live=python

///////////////////////////////////////////////////////////////////////////////

// GetRange makes a streaming request for timeseries data from Databento.
//
// This method returns the byte array of the DBN stream.
//
// # Errors
// This function returns an error when it fails to communicate with the Databento API
// or the API indicates there's an issue with the request.
func GetRange(apiKey string, jobParams SubmitJobParams) ([]byte, error) {
	apiUrl := "https://hist.databento.com/v0/timeseries.get_range"

	formData := url.Values{}
	err := jobParams.ApplyToURLValues(&formData)
	if err != nil {
		return nil, fmt.Errorf("bad params: %w", err)
	}

	body, err := databentoPostFormRequest(apiUrl, apiKey, formData, "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("failed post request: %w", err)
	}

	return body, nil
}

///////////////////////////////////////////////////////////////////////////////

// GetRangeStream makes a streaming request for timeseries data from Databento.
//
// Returns an io.ReadCloser for the DBN stream. The caller must close the reader when done.
// For zstd-compressed responses, wrap with zstd.NewReader before passing to dbn.NewDbnScanner.
// Use io.Copy to stream directly to a file without buffering in memory.
//
// # Errors
// This function returns an error when it fails to communicate with the Databento API
// or the API indicates there's an issue with the request.
func GetRangeStream(apiKey string, jobParams SubmitJobParams) (io.ReadCloser, error) {
	apiUrl := "https://hist.databento.com/v0/timeseries.get_range"

	formData := url.Values{}
	err := jobParams.ApplyToURLValues(&formData)
	if err != nil {
		return nil, fmt.Errorf("bad params: %w", err)
	}

	body, err := databentoPostFormStream(apiUrl, apiKey, formData, "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("failed post request: %w", err)
	}

	return body, nil
}
