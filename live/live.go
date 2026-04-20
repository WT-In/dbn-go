// Copyright (c) 2024 Neomantra Corp

// TODO: better state machine management (authenticated, started, stopped)

package dbn_live

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/WT-In/dbn-go"
)

const (
	DATABENTO_VERSION = "0.18.1"

	DATABENTO_API_ENV_KEY    = "DATABENTO_API_KEY"
	DATABENTO_CLIENT_ENV_KEY = "DATABENTO_CLIENT"

	LIVE_HOST_SUFFIX = ".lsg.databento.com"
	LIVE_API_PORT    = 13000

	SYSTEM_MSG_SIZE_V1 = 64
	SYSTEM_MSG_SIZE_V2 = 303

	ERROR_ERR_SIZE_V1 = 64
	ERROR_ERR_SIZE_V2 = 302

	BUCKET_ID_LENGTH = 5

	API_VERSION     = 0
	API_VERSION_STR = "0"
	API_KEY_LENGTH  = 32

	MAX_STR_LENGTH = 24 * 1024

	// SUBSCRIPTION_LINE_MAX is the maximum number of bytes in a single
	// subscription-request line on the wire. Matches the live gateway line-size
	// ceiling and the reader buffer size (MAX_STR_LENGTH).
	SUBSCRIPTION_LINE_MAX = MAX_STR_LENGTH
)

// deadlineReader applies a per-read deadline to every Read. The deadline
// duration can be swapped atomically; a zero duration disables the deadline
// for subsequent reads.
//
// When ReadTimeout is used in steady state, a healthy stream must deliver bytes
// within that duration on every read (heartbeats fill idle periods per Databento
// gateway behavior).
type deadlineReader struct {
	conn    net.Conn
	timeout atomic.Int64 // nanoseconds; 0 disables the deadline
}

func (r *deadlineReader) Read(p []byte) (int, error) {
	if d := r.timeout.Load(); d > 0 {
		if err := r.conn.SetReadDeadline(time.Now().Add(time.Duration(d))); err != nil {
			return 0, err
		}
	} else {
		// Clear any deadline left from a previous positive timeout so reads block
		// until data is available again.
		if err := r.conn.SetReadDeadline(time.Time{}); err != nil {
			return 0, err
		}
	}
	return r.conn.Read(p)
}

func (r *deadlineReader) setTimeout(d time.Duration) {
	if d < 0 {
		d = 0
	}
	r.timeout.Store(int64(d))
}

func newBufReader(conn net.Conn, cfg LiveConfig) (*bufio.Reader, *deadlineReader) {
	if cfg.ReadTimeout == 0 && cfg.HandshakeReadTimeout == 0 {
		return bufio.NewReaderSize(conn, MAX_STR_LENGTH), nil
	}
	dr := &deadlineReader{conn: conn}
	dr.setTimeout(cfg.HandshakeReadTimeout)
	return bufio.NewReaderSize(dr, MAX_STR_LENGTH), dr
}

type SystemMsgV1 struct {
	Msg [SYSTEM_MSG_SIZE_V1]byte `json:"msg"` // The message from the gateway
}

type ErrorMsgV1 struct {
	Err [ERROR_ERR_SIZE_V1]byte `json:"err"` // The error message
}

type SystemMsgV2 struct {
	Msg  [SYSTEM_MSG_SIZE_V2]byte `json:"msg"`  // The message from the gateway
	Code uint8                    `json:"code"` // Reserved for future use
}

type ErrorMsgV2 struct {
	Err    [ERROR_ERR_SIZE_V2]byte `json:"err"`     // The error message
	Code   uint8                   `json:"code"`    // Reserved for future use
	IsLast uint8                   `json:"is_last"` // Boolean flag indicating whther this is the last in a series of error records.
}

///////////////////////////////////////////////////////////////////////////////

type LiveConfig struct {
	Logger            *slog.Logger
	ApiKey            string
	Dataset           string
	Client            string
	Encoding          dbn.Encoding // nil mean Encoding_Dbn
	SendTsOut         bool
	HeartbeatInterval time.Duration // Heartbeat interval; 0 means use server default
	// ReadTimeout bounds the maximum idle gap between bytes while streaming records
	// (after session start and DBN metadata). Heartbeats from the gateway should
	// arrive within this window. Zero disables per-read deadlines in steady state.
	ReadTimeout time.Duration
	// HandshakeReadTimeout bounds idle gaps during the greeting, CRAM challenge,
	// authentication response, and DBN metadata read. Large symbol lists can delay
	// metadata; use this instead of tightening ReadTimeout for that phase. Zero
	// disables per-read deadlines during handshake (default).
	HandshakeReadTimeout time.Duration
	SlowReaderBehavior   dbn.SlowReaderBehavior
	VersionUpgradePolicy dbn.VersionUpgradePolicy
	Verbose              bool
}

// SetFromEnv fills in the LiveConfig from environment variables.
// `DATABENTO_API_KEY` holds the Databento API key.
// `DATABENTO_CLIENT` holds the Client name.
func (c *LiveConfig) SetFromEnv() error {
	databentoApiKey := os.Getenv(DATABENTO_API_ENV_KEY)
	if databentoApiKey == "" {
		return errors.New("expected environment variable DATABENTO_API_KEY to be set")
	}
	c.ApiKey = databentoApiKey

	if c.Client == "" {
		c.Client = os.Getenv(DATABENTO_CLIENT_ENV_KEY)
	}
	return nil
}

func (c *LiveConfig) validate() error {
	if len(c.ApiKey) == 0 {
		return errors.New("field ApiKey is unset")
	}
	if len(c.ApiKey) != API_KEY_LENGTH {
		return fmt.Errorf("field ApiKey must contain %d characters", API_KEY_LENGTH)
	}
	if len(c.Dataset) == 0 {
		return errors.New("field Dataset is unset")
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////

// LiveClient interfaces with Databento's real-time and intraday replay
// market data API. This client provides a blocking API for getting the next
// record. Unlike Historical, each instance of LiveClient is associated with a
// particular dataset.
type LiveClient struct {
	config  LiveConfig
	gateway string
	port    uint16

	logger *slog.Logger

	conn      net.Conn
	bufReader *bufio.Reader

	deadlineReader *deadlineReader

	dbnScanner  *dbn.DbnScanner
	jsonScanner *dbn.JsonScanner

	lsgVersion string
	sessionID  string
}

// NewLiveClient takes a LiveConfig, creates a LiveClient and tries to connect.
// Returns an error if connection fails.
func NewLiveClient(config LiveConfig) (*LiveClient, error) {
	if config.validate() != nil {
		return nil, fmt.Errorf("invalid config: %v", config.validate())
	}

	c := &LiveClient{
		config:  config,
		gateway: dbn.DatasetToHostname(config.Dataset) + LIVE_HOST_SUFFIX,
		port:    LIVE_API_PORT,
		logger:  config.Logger,
	}

	if c.logger == nil {
		c.logger = slog.Default()
	}

	if c.config.Client == "" {
		c.config.Client = "Go " + DATABENTO_VERSION
	}

	// Connect to server
	hostPort := net.JoinHostPort(c.gateway, strconv.FormatUint(uint64(c.port), 10))
	if conn, err := net.Dial("tcp", hostPort); err != nil {
		return nil, err
	} else {
		c.conn = conn
	}
	c.bufReader, c.deadlineReader = newBufReader(c.conn, config)
	if c.config.Verbose {
		c.logger.Info("[LiveClient.NewLiveClient] connected", "dataset", config.Dataset, "hostport", hostPort)
	}
	return c, nil
}

// GetConfig returns the LiveConfig used to create the LiveClient.
func (c *LiveClient) GetConfig() LiveConfig {
	return c.config
}

// GetGateway returns the gateway host for connection.
func (c *LiveClient) GetGateway() string {
	return c.gateway
}

// GetPort returns the port for connection.
func (c *LiveClient) GetPort() uint16 {
	return c.port
}

// GetLsgVersion returns the version of the LSG server
func (c *LiveClient) GetLsgVersion() string {
	return c.lsgVersion
}

// GetSessionID returns the session ID
func (c *LiveClient) GetSessionID() string {
	return c.sessionID
}

// GetDbnScanner returns the DbnScanner
// Returns nil if the encoding is JSON
func (c *LiveClient) GetDbnScanner() *dbn.DbnScanner {
	return c.dbnScanner
}

// GetJsonScanner returns a JsonScanner
// Returns nil if the encoding is DBN
func (c *LiveClient) GetJsonScanner() *dbn.JsonScanner {
	return c.jsonScanner
}

///////////////////////////////////////////////////////////////////////////////

// Subscribe adds a new subscription for a set of symbols with a given schema and stype.
// Returns an error if any.
// A single client instance supports multiple
// subscriptions. Note there is no unsubscribe method. Subscriptions end
// when the client disconnects with Stop or the LiveClient instance is garbage collected.
func (c *LiveClient) Subscribe(sub SubscriptionRequestMsg) error {
	if len(sub.Symbols) == 0 {
		return errors.New("subscribe request must contain at least one symbol")
	}
	fixedLen := len(sub.encodeChunk(nil, true))
	if fixedLen >= SUBSCRIPTION_LINE_MAX {
		return fmt.Errorf("subscription request fixed fields exceed max line length (%d bytes)", SUBSCRIPTION_LINE_MAX)
	}
	budget := SUBSCRIPTION_LINE_MAX - fixedLen

	var chunk []string
	used := 0
	flush := func(isLast bool) error {
		line := sub.encodeChunk(chunk, isLast)
		if len(line) > SUBSCRIPTION_LINE_MAX {
			return fmt.Errorf("internal error: subscription line length %d exceeds max %d", len(line), SUBSCRIPTION_LINE_MAX)
		}
		n, err := c.conn.Write(line)
		if err != nil {
			return fmt.Errorf("failed to send subscribe request: %w", err)
		}
		if n != len(line) {
			return fmt.Errorf("failed to send subscribe request: wanted %d sent %d", len(line), n)
		}
		chunk = chunk[:0]
		used = 0
		return nil
	}

	for _, sym := range sub.Symbols {
		cost := len(sym)
		if len(chunk) > 0 {
			cost++ // comma between symbols
		}
		if cost > budget {
			return fmt.Errorf("symbol %q exceeds max subscription symbol payload (%d bytes)", sym, budget)
		}
		if used+cost > budget {
			if err := flush(false); err != nil {
				return err
			}
			cost = len(sym)
		}
		chunk = append(chunk, sym)
		used += cost
	}

	if err := flush(true); err != nil {
		return err
	}

	if c.config.Verbose {
		symbols := strings.Join(sub.Symbols, ",")
		c.logger.Info("[LiveClient.Subscribe]",
			"schema", sub.Schema, "start", sub.Start,
			"stype_in", sub.StypeIn.String(), "symbols", symbols,
		)
	}

	return nil
}

// Notifies the gateway to start sending messages for all subscriptions.
// This method should only be called once per instance.
func (c *LiveClient) Start() error {
	// TODO: don't start twice, etc
	// Send start_session
	msg := SessionStartMsg{}
	startBytes := msg.Encode()
	if n, err := c.conn.Write(startBytes); err != nil {
		return fmt.Errorf("failed to send start: %v", err)
	} else if n != len(startBytes) {
		return fmt.Errorf("failed to send start: wanted %d sent %d", len(startBytes), n)
	}
	if c.config.Verbose {
		c.logger.Info("[LiveClient.Start] sent start_session")
	}

	if c.bufReader == nil {
		c.bufReader, c.deadlineReader = newBufReader(c.conn, c.config)
	}

	// Create a DbnScanner and ensure we get the metadata
	if c.config.Encoding == dbn.Encoding_Json {
		c.jsonScanner = dbn.NewJsonScanner(c.bufReader)
	} else {
		c.dbnScanner = dbn.NewDbnScanner(c.bufReader)
		metadata, err := c.dbnScanner.Metadata()
		if err != nil {
			return fmt.Errorf("failed to get metadata: %v", err)
		}
		if c.config.Verbose {
			c.logger.Info("[LiveClient.Start] read metadata successfully",
				"version_num", metadata.VersionNum, "len_mappings", len(metadata.Mappings))
		}
	}

	if c.deadlineReader != nil {
		c.deadlineReader.setTimeout(c.config.ReadTimeout)
	}

	return nil
}

// Stops the session with the gateway. Once stopped, the session cannot be restarted.
func (c *LiveClient) Stop() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		if err != nil {
			if c.config.Verbose {
				c.logger.Error("[LiveClient.Stop] error closing connection", "error", err.Error())
			}
			return err
		} else {
			if c.config.Verbose {
				c.logger.Info("[LiveClient.Stop] closed connection")
			}
		}
	}
	if c.config.Verbose {
		c.logger.Info("[LiveClient.Stop] stopped")
	}
	return nil
}

// Authenticate performs read/write with the server to authenticate.
// Returns a sessionID or an error.
func (c *LiveClient) Authenticate(apiKey string) (string, error) {
	// Read challege from socket and calcluate reply
	challengeKey, err := c.decodeChallenge()
	if err != nil {
		return "", err
	}

	auth := generateCramReply(apiKey, challengeKey)

	// Write out the auth request
	request := AuthenticationRequestMsg{
		Auth:               auth,
		Dataset:            c.config.Dataset,
		Encoding:           c.config.Encoding,
		TsOut:              c.config.SendTsOut,
		Client:             c.config.Client,
		HeartbeatIntervalS: uint32(c.config.HeartbeatInterval.Seconds()),
		SlowReaderBehavior: c.config.SlowReaderBehavior,
	}
	requestBytes := request.Encode()
	if n, err := c.conn.Write(requestBytes); err != nil {
		return "", fmt.Errorf("failed to send auth request: %v", err)
	} else if n != len(requestBytes) {
		return "", fmt.Errorf("failed to send auth request: wanted %d sent %d", len(requestBytes), n)
	}

	// Read the response
	sessionID, err := c.decodeAuthResponse()
	if err != nil {
		return "", err
	}
	c.sessionID = sessionID
	if c.config.Verbose {
		c.logger.Info("[LiveClient.Authenticate] Successfully authenticated", "session_id", sessionID)
	}
	return sessionID, nil
}

// https://databento.com/docs/api-reference-live/message-flows?historical=http&live=raw
func (c *LiveClient) decodeChallenge() (string, error) {
	// first line is version
	line, err := c.bufReader.ReadBytes('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read Greeting: %w", err)
	}
	greeting := NewGreetingMsgFromBytes(line)
	if greeting == nil {
		return "", errors.New("failed to parse greeting")
	}
	c.lsgVersion = greeting.LsgVersion
	if c.config.Verbose {
		c.logger.Info("[LiveClient.decodeChallenge]", "version", greeting.LsgVersion)
	}

	// next is challenge request
	line, err = c.bufReader.ReadBytes('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read Challenge: %w", err)
	}
	challenge := NewChallengeRequestMsgFromBytes(line)
	if challenge == nil {
		return "", errors.New("failed to parse challenge")
	}
	if c.config.Verbose {
		c.logger.Info("[LiveClient.decodeChallenge]", "cram", challenge.Cram)
	}

	return challenge.Cram, nil
}

func (c *LiveClient) decodeAuthResponse() (string, error) {
	line, err := c.bufReader.ReadBytes('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read AuthResponse: %w", err)
	}
	resp := NewAuthenticationResponseMsgFromBytes(line)
	if resp == nil {
		return "", errors.New("failed to parse AuthResponse")
	}

	if c.config.Verbose {
		c.logger.Info("[LiveClient.decodeAuthResponse", "success", resp.Success, "error", resp.Error, "session_id", resp.SessionID)
	}

	if resp.Success == "0" {
		return "", fmt.Errorf("failed to authenticate: error: %s", resp.Error)
	}

	return resp.SessionID, nil
}

func generateCramReply(apiKey string, challengeKey string) string {
	request := fmt.Sprintf("%s|%s", challengeKey, apiKey)

	hasher := sha256.New()
	hasher.Write([]byte(request))
	checksum := hasher.Sum(nil)

	firstKeyIndex := API_KEY_LENGTH - BUCKET_ID_LENGTH
	bucketID := apiKey[firstKeyIndex:]
	return fmt.Sprintf("%x-%s", checksum, bucketID)
}
