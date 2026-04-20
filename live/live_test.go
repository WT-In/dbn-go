// Copyright (c) 2024 Neomantra Corp

package dbn_live

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Test Launcher
func TestDbnLive(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dbn-go live suite")
}

var _ = Describe("DbnLive", func() {
	Context("auth", func() {
		It("should generate CRAM response properly", func() {
			// https://databento.com/docs/api-reference-live/message-flows/authentication/example?historical=http&live=raw
			apiKey := "db-89s9vCvwDDKPdQJ5Pb30Fyj9mNUM6"
			cram := "j5pwMHz6vwXruJM4cOwQrQeQE0bImIzT"
			expected := "6d3c875bb9f8cf503c3ed83ee5f476a3ad53f0c67706c51cf42d2db5ad8ff5a9-mNUM6"

			resp := generateCramReply(apiKey, cram)
			Expect(resp).To(Equal(expected))
		})
	})

	Context("AuthenticationRequestMsg.Encode", func() {
		It("should encode only required fields and client when all defaults", func() {
			msg := AuthenticationRequestMsg{
				Auth:    "my-auth",
				Dataset: "XNAS.ITCH",
				Client:  "test-client",
			}
			encoded := string(msg.Encode())
			Expect(encoded).To(Equal("auth=my-auth|dataset=XNAS.ITCH|client=test-client\n"))
		})

		It("should omit encoding when dbn (default)", func() {
			msg := AuthenticationRequestMsg{
				Auth:     "a",
				Dataset:  "d",
				Client:   "c",
				Encoding: dbn.Encoding_Dbn,
			}
			Expect(string(msg.Encode())).ToNot(ContainSubstring("encoding="))
		})

		It("should include encoding when non-default", func() {
			msg := AuthenticationRequestMsg{
				Auth:     "a",
				Dataset:  "d",
				Client:   "c",
				Encoding: dbn.Encoding_Json,
			}
			Expect(string(msg.Encode())).To(ContainSubstring("|encoding=json"))
		})

		It("should include compression when non-default", func() {
			msg := AuthenticationRequestMsg{
				Auth:        "a",
				Dataset:     "d",
				Client:      "c",
				Compression: dbn.Compress_ZStd,
			}
			Expect(string(msg.Encode())).To(ContainSubstring("|compression=zstd"))
		})

		It("should include boolean flags only when true", func() {
			msg := AuthenticationRequestMsg{
				Auth:     "a",
				Dataset:  "d",
				Client:   "c",
				TsOut:    true,
				PrettyPx: true,
				PrettyTs: true,
			}
			encoded := string(msg.Encode())
			Expect(encoded).To(ContainSubstring("|ts_out=1"))
			Expect(encoded).To(ContainSubstring("|pretty_px=1"))
			Expect(encoded).To(ContainSubstring("|pretty_ts=1"))
		})

		It("should omit boolean flags when false", func() {
			msg := AuthenticationRequestMsg{
				Auth:    "a",
				Dataset: "d",
				Client:  "c",
			}
			encoded := string(msg.Encode())
			Expect(encoded).ToNot(ContainSubstring("ts_out"))
			Expect(encoded).ToNot(ContainSubstring("pretty_px"))
			Expect(encoded).ToNot(ContainSubstring("pretty_ts"))
		})

		It("should include heartbeat_interval_s when non-zero", func() {
			msg := AuthenticationRequestMsg{
				Auth:               "a",
				Dataset:            "d",
				Client:             "c",
				HeartbeatIntervalS: 10,
			}
			Expect(string(msg.Encode())).To(ContainSubstring("|heartbeat_interval_s=10"))
		})

		It("should omit heartbeat_interval_s when zero", func() {
			msg := AuthenticationRequestMsg{
				Auth:    "a",
				Dataset: "d",
				Client:  "c",
			}
			Expect(string(msg.Encode())).ToNot(ContainSubstring("heartbeat_interval_s"))
		})

		It("should include slow_reader_behavior when skip (non-default)", func() {
			msg := AuthenticationRequestMsg{
				Auth:               "a",
				Dataset:            "d",
				Client:             "c",
				SlowReaderBehavior: dbn.SlowReaderBehavior_Skip,
			}
			Expect(string(msg.Encode())).To(ContainSubstring("|slow_reader_behavior=skip"))
		})

		It("should omit slow_reader_behavior when warn (default)", func() {
			msg := AuthenticationRequestMsg{
				Auth:               "a",
				Dataset:            "d",
				Client:             "c",
				SlowReaderBehavior: dbn.SlowReaderBehavior_Warn,
			}
			Expect(string(msg.Encode())).ToNot(ContainSubstring("slow_reader_behavior"))
		})

		It("should encode all non-default fields together", func() {
			msg := AuthenticationRequestMsg{
				Auth:               "my-auth",
				Dataset:            "GLBX.MDP3",
				Client:             "my-client",
				Encoding:           dbn.Encoding_Json,
				Compression:        dbn.Compress_ZStd,
				TsOut:              true,
				PrettyPx:           true,
				PrettyTs:           true,
				HeartbeatIntervalS: 30,
				SlowReaderBehavior: dbn.SlowReaderBehavior_Skip,
			}
			encoded := string(msg.Encode())
			Expect(encoded).To(HavePrefix("auth=my-auth|dataset=GLBX.MDP3|client=my-client"))
			Expect(encoded).To(ContainSubstring("|encoding=json"))
			Expect(encoded).To(ContainSubstring("|compression=zstd"))
			Expect(encoded).To(ContainSubstring("|ts_out=1"))
			Expect(encoded).To(ContainSubstring("|pretty_px=1"))
			Expect(encoded).To(ContainSubstring("|pretty_ts=1"))
			Expect(encoded).To(ContainSubstring("|heartbeat_interval_s=30"))
			Expect(encoded).To(ContainSubstring("|slow_reader_behavior=skip"))
			Expect(encoded).To(HaveSuffix("\n"))
		})
	})

	Context("control messages", func() {
		It("should not panic on empty input", func() {
			Expect(func() {
				_ = NewGreetingMsgFromBytes([]byte{})
			}).ToNot(Panic())
			Expect(NewGreetingMsgFromBytes([]byte{})).To(BeNil())
		})
	})

	Context("getters", func() {
		It("should return session ID from GetSessionID", func() {
			client := &LiveClient{
				lsgVersion: "1.2.3",
				sessionID:  "sess-123",
			}
			Expect(client.GetSessionID()).To(Equal("sess-123"))
		})
	})

	Context("SubscriptionRequestMsg.Encode", func() {
		It("should end with is_last=1 and a newline", func() {
			msg := SubscriptionRequestMsg{
				Schema:  "mbp-1",
				StypeIn: dbn.SType_RawSymbol,
				Symbols: []string{"AAPL", "MSFT"},
			}
			encoded := string(msg.Encode())
			Expect(encoded).To(HaveSuffix("|is_last=1\n"))
			Expect(encoded).To(ContainSubstring("|symbols=AAPL,MSFT|"))
		})
	})

	Context("Subscribe chunking", func() {
		It("should emit multiple lines with is_last=0 until final is_last=1", func() {
			base := SubscriptionRequestMsg{
				Schema:  "s",
				StypeIn: dbn.SType_RawSymbol,
			}
			fixed := len(base.encodeChunk(nil, true))
			budget := SUBSCRIPTION_LINE_MAX - fixed
			Expect(budget).To(BeNumerically(">", 8))

			n := budget*2 + 5
			syms := make([]string, n)
			for i := range syms {
				syms[i] = "a"
			}
			sub := SubscriptionRequestMsg{
				Schema:  "s",
				StypeIn: dbn.SType_RawSymbol,
				Symbols: syms,
			}
			conn := &writeCapture{}
			client := &LiveClient{conn: conn}
			Expect(client.Subscribe(sub)).To(Succeed())

			var nonEmpty [][]byte
			for _, ln := range bytes.Split(conn.buf.Bytes(), []byte{'\n'}) {
				if len(ln) > 0 {
					nonEmpty = append(nonEmpty, ln)
				}
			}
			Expect(len(nonEmpty)).To(BeNumerically(">=", 3))
			for i, ln := range nonEmpty {
				Expect(len(ln)).To(BeNumerically("<=", SUBSCRIPTION_LINE_MAX))
				s := string(ln)
				if i < len(nonEmpty)-1 {
					Expect(s).To(ContainSubstring("is_last=0"))
				} else {
					Expect(s).To(ContainSubstring("is_last=1"))
				}
			}
		})

		It("should reject a symbol larger than the line budget", func() {
			base := SubscriptionRequestMsg{
				Schema:  "x",
				StypeIn: dbn.SType_RawSymbol,
			}
			budget := SUBSCRIPTION_LINE_MAX - len(base.encodeChunk(nil, true))
			big := strings.Repeat("Z", budget+1)
			conn := &writeCapture{}
			client := &LiveClient{conn: conn}
			err := client.Subscribe(SubscriptionRequestMsg{
				Schema:  "x",
				StypeIn: dbn.SType_RawSymbol,
				Symbols: []string{big},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds max subscription symbol payload"))
			Expect(conn.buf.Len()).To(Equal(0))
		})
	})

	Context("deadlineReader", func() {
		It("should stop applying read deadlines after setTimeout(0)", func() {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			dr := &deadlineReader{conn: clientConn}
			dr.setTimeout(25 * time.Millisecond)

			errCh := make(chan error, 1)
			go func() {
				buf := make([]byte, 16)
				_, err := dr.Read(buf)
				errCh <- err
			}()

			var err error
			select {
			case err = <-errCh:
			case <-time.After(time.Second):
				Fail("timed out waiting for deadline read to return")
			}
			Expect(err).To(HaveOccurred())
			var netErr net.Error
			Expect(errors.As(err, &netErr)).To(BeTrue())
			Expect(netErr.Timeout()).To(BeTrue())

			dr.setTimeout(0)
			go func() {
				time.Sleep(40 * time.Millisecond)
				_, _ = serverConn.Write([]byte("ping"))
			}()
			buf := make([]byte, 4)
			n, err := dr.Read(buf)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(4))
			Expect(string(buf)).To(Equal("ping"))
		})
	})

	Context("authenticate", func() {
		It("should send configured client in auth request and store session ID", func() {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			client := &LiveClient{
				config: LiveConfig{
					Dataset:  "XNAS.ITCH",
					Encoding: dbn.Encoding_Dbn,
					Client:   "dbn-go-live-test",
				},
				conn:      clientConn,
				bufReader: bufio.NewReaderSize(clientConn, MAX_STR_LENGTH),
			}

			serverErr := make(chan error, 1)
			authLine := make(chan string, 1)
			go func() {
				reader := bufio.NewReader(serverConn)
				if _, err := serverConn.Write([]byte("lsg_version=1.0\n")); err != nil {
					serverErr <- err
					return
				}
				if _, err := serverConn.Write([]byte("cram=challenge-value\n")); err != nil {
					serverErr <- err
					return
				}
				line, err := reader.ReadString('\n')
				if err != nil {
					serverErr <- err
					return
				}
				authLine <- line
				if _, err := serverConn.Write([]byte("success=1|session_id=sess-42\n")); err != nil {
					serverErr <- err
					return
				}
				serverErr <- nil
			}()

			sessionID, err := client.Authenticate("db-89s9vCvwDDKPdQJ5Pb30Fyj9mNUM6")
			Expect(err).To(BeNil())
			Expect(sessionID).To(Equal("sess-42"))
			Expect(client.GetSessionID()).To(Equal("sess-42"))

			line := <-authLine
			Expect(line).To(ContainSubstring("dataset=XNAS.ITCH"))
			Expect(line).To(ContainSubstring("client=dbn-go-live-test"))
			Expect(line).To(ContainSubstring("auth="))

			Expect(<-serverErr).To(BeNil())
		})

		It("should return error on failed authentication response", func() {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			client := &LiveClient{
				config: LiveConfig{
					Dataset:  "XNAS.ITCH",
					Encoding: dbn.Encoding_Dbn,
					Client:   "dbn-go-live-test",
				},
				conn:      clientConn,
				bufReader: bufio.NewReaderSize(clientConn, MAX_STR_LENGTH),
			}

			go func() {
				reader := bufio.NewReader(serverConn)
				serverConn.Write([]byte("lsg_version=1.0\n"))
				serverConn.Write([]byte("cram=challenge-value\n"))
				_, _ = reader.ReadString('\n') // consume auth request
				serverConn.Write([]byte("success=0|error=bad credentials\n"))
			}()

			_, err := client.Authenticate("db-89s9vCvwDDKPdQJ5Pb30Fyj9mNUM6")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to authenticate"))
		})
	})

	Context("start", func() {
		It("should preserve buffered DBN bytes after authentication", func() {
			dbnBytes, err := os.ReadFile(filepath.Join("..", "tests", "data", "test_data.ohlcv-1s.dbn"))
			Expect(err).To(BeNil())

			conn := &scriptedConn{
				reads: [][]byte{
					[]byte("lsg_version=1.0\n"),
					[]byte("cram=challenge-value\n"),
					append([]byte("success=1|session_id=sess-42\n"), dbnBytes...),
				},
			}

			client := &LiveClient{
				config: LiveConfig{
					Dataset:  "GLBX.MDP3",
					Encoding: dbn.Encoding_Dbn,
				},
				conn:      conn,
				bufReader: bufio.NewReaderSize(conn, MAX_STR_LENGTH),
			}

			sessionID, err := client.Authenticate("db-89s9vCvwDDKPdQJ5Pb30Fyj9mNUM6")
			Expect(err).To(BeNil())
			Expect(sessionID).To(Equal("sess-42"))

			err = client.Start()
			Expect(err).To(BeNil())
			Expect(conn.writes.String()).To(ContainSubstring("start_session="))

			scanner := client.GetDbnScanner()
			Expect(scanner).ToNot(BeNil())
			Expect(scanner.Next()).To(BeTrue())

			record, err := dbn.DbnScannerDecode[dbn.OhlcvMsg](scanner)
			Expect(err).To(BeNil())
			Expect(record.Header.InstrumentID).To(Equal(uint32(5482)))
		})

		It("should preserve buffered JSON bytes after authentication", func() {
			jsonBytes, err := os.ReadFile(filepath.Join("..", "tests", "data", "test_data.ohlcv-1s.json"))
			Expect(err).To(BeNil())

			firstLine, _, ok := bytes.Cut(jsonBytes, []byte{'\n'})
			Expect(ok).To(BeTrue())

			conn := &scriptedConn{
				reads: [][]byte{
					[]byte("lsg_version=1.0\n"),
					[]byte("cram=challenge-value\n"),
					append([]byte("success=1|session_id=sess-43\n"), append(firstLine, '\n')...),
				},
			}

			client := &LiveClient{
				config: LiveConfig{
					Dataset:  "GLBX.MDP3",
					Encoding: dbn.Encoding_Json,
				},
				conn:      conn,
				bufReader: bufio.NewReaderSize(conn, MAX_STR_LENGTH),
			}

			sessionID, err := client.Authenticate("db-89s9vCvwDDKPdQJ5Pb30Fyj9mNUM6")
			Expect(err).To(BeNil())
			Expect(sessionID).To(Equal("sess-43"))

			err = client.Start()
			Expect(err).To(BeNil())

			scanner := client.GetJsonScanner()
			Expect(scanner).ToNot(BeNil())
			Expect(scanner.Next()).To(BeTrue())

			record, err := dbn.JsonScannerDecode[dbn.OhlcvMsg](scanner)
			Expect(err).To(BeNil())
			Expect(record.Header.InstrumentID).To(Equal(uint32(5482)))
		})
	})
})

type writeCapture struct {
	buf bytes.Buffer
}

func (w *writeCapture) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (w *writeCapture) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *writeCapture) Close() error {
	return nil
}

func (w *writeCapture) LocalAddr() net.Addr {
	return dummyAddr("local")
}

func (w *writeCapture) RemoteAddr() net.Addr {
	return dummyAddr("remote")
}

func (w *writeCapture) SetDeadline(time.Time) error {
	return nil
}

func (w *writeCapture) SetReadDeadline(time.Time) error {
	return nil
}

func (w *writeCapture) SetWriteDeadline(time.Time) error {
	return nil
}

type scriptedConn struct {
	reads   [][]byte
	pending []byte
	writes  bytes.Buffer
	closed  bool
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		if len(c.reads) == 0 {
			return 0, io.EOF
		}
		c.pending = c.reads[0]
		c.reads = c.reads[1:]
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.writes.Write(p)
}

func (c *scriptedConn) Close() error {
	c.closed = true
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr {
	return dummyAddr("local")
}

func (c *scriptedConn) RemoteAddr() net.Addr {
	return dummyAddr("remote")
}

func (c *scriptedConn) SetDeadline(time.Time) error {
	return nil
}

func (c *scriptedConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *scriptedConn) SetWriteDeadline(time.Time) error {
	return nil
}

type dummyAddr string

func (a dummyAddr) Network() string {
	return string(a)
}

func (a dummyAddr) String() string {
	return string(a)
}
