// Copyright (c) 2024 Neomantra Corp
//
// Tests for SystemMsg decoding, including V1→V2 upgrade with text-pattern
// SystemCode inference and the IsHeartbeat string fallback.

package dbn_test

import (
	dbn "github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// makeV1SystemMsgWire builds the on-wire bytes of a DBN v1 SystemMsg
// (16-byte header + 64-byte msg, no code byte).
func makeV1SystemMsgWire(msg string) []byte {
	b := make([]byte, dbn.SystemMsgV1_Size)
	b[0] = uint8(dbn.SystemMsgV1_Size / 4) // length in 4-byte words
	b[1] = uint8(dbn.RType_System)
	copy(b[16:80], []byte(msg))
	return b
}

// makeV2SystemMsgWire builds the on-wire bytes of a DBN v2 SystemMsg
// (16-byte header + 303-byte msg + 1-byte code).
func makeV2SystemMsgWire(msg string, code dbn.SystemCode) []byte {
	b := make([]byte, dbn.SystemMsg_Size)
	b[0] = uint8(dbn.SystemMsg_Size / 4) // length in 4-byte words
	b[1] = uint8(dbn.RType_System)
	copy(b[16:16+dbn.SystemMsg_MsgSize], []byte(msg))
	b[16+dbn.SystemMsg_MsgSize] = uint8(code)
	return b
}

var _ = Describe("SystemMsg", func() {
	Context("size constants", func() {
		It("V1 SystemMsg is 80 bytes (16 header + 64 msg, no code)", func() {
			Expect(dbn.SystemMsgV1_Size).To(BeEquivalentTo(16 + 64))
			Expect(dbn.SystemMsgV1_MsgSize).To(BeEquivalentTo(64))
		})

		It("V2 SystemMsg is 320 bytes (16 header + 303 msg + 1 code)", func() {
			Expect(dbn.SystemMsg_Size).To(BeEquivalentTo(16 + 303 + 1))
		})
	})

	Context("DecodeSystemMsg with V1 metadata", func() {
		meta := &dbn.Metadata{VersionNum: dbn.HeaderVersion1}

		DescribeTable(
			"infers SystemCode from the message text (matching databento/dbn v1→v2 conversion)",
			func(msg string, want dbn.SystemCode) {
				rec, err := dbn.DecodeSystemMsg(meta, makeV1SystemMsgWire(msg))
				Expect(err).To(BeNil())
				Expect(rec.Code).To(Equal(want))
				Expect(dbn.TrimNullBytes(rec.Message[:])).To(Equal(msg))
			},
			Entry("HEARTBEAT", "HEARTBEAT", dbn.SystemCode(dbn.SystemCode_Heartbeat)),
			Entry("SubscriptionAck",
				"Subscription request 7 for mbp-1 data succeeded",
				dbn.SystemCode(dbn.SystemCode_SubscriptionAck)),
			Entry("SlowReaderWarning",
				"Warning: slow reading detected on dataset GLBX.MDP3",
				dbn.SystemCode(dbn.SystemCode_SlowReaderWarning)),
			Entry("ReplayCompleted",
				"Finished 3 replay",
				dbn.SystemCode(dbn.SystemCode_ReplayCompleted)),
			Entry("EndOfInterval",
				"End of interval for 2026-05-16T12:00:00Z",
				dbn.SystemCode(dbn.SystemCode_EndOfInterval)),
			Entry("unrecognized text → Unset",
				"some unrecognized server message",
				dbn.SystemCode(dbn.SystemCode_Unset)),
		)

		It("IsHeartbeat is true only for the HEARTBEAT text", func() {
			hb, err := dbn.DecodeSystemMsg(meta, makeV1SystemMsgWire("HEARTBEAT"))
			Expect(err).To(BeNil())
			Expect(hb.IsHeartbeat()).To(BeTrue())

			ack, err := dbn.DecodeSystemMsg(meta, makeV1SystemMsgWire(
				"Subscription request 1 for mbp-1 data succeeded"))
			Expect(err).To(BeNil())
			Expect(ack.IsHeartbeat()).To(BeFalse())
		})
	})

	Context("DecodeSystemMsg with V2 metadata", func() {
		meta := &dbn.Metadata{VersionNum: dbn.HeaderVersion2}

		DescribeTable(
			"reads the explicit code byte verbatim",
			func(code dbn.SystemCode) {
				wire := makeV2SystemMsgWire("server message text", code)
				rec, err := dbn.DecodeSystemMsg(meta, wire)
				Expect(err).To(BeNil())
				Expect(rec.Code).To(Equal(code))
			},
			Entry("Heartbeat", dbn.SystemCode(dbn.SystemCode_Heartbeat)),
			Entry("SubscriptionAck", dbn.SystemCode(dbn.SystemCode_SubscriptionAck)),
			Entry("SlowReaderWarning", dbn.SystemCode(dbn.SystemCode_SlowReaderWarning)),
			Entry("ReplayCompleted", dbn.SystemCode(dbn.SystemCode_ReplayCompleted)),
			Entry("EndOfInterval", dbn.SystemCode(dbn.SystemCode_EndOfInterval)),
		)
	})

	Context("DecodeSystemMsg error handling", func() {
		It("rejects unknown DBN versions", func() {
			meta := &dbn.Metadata{VersionNum: 99}
			_, err := dbn.DecodeSystemMsg(meta, makeV1SystemMsgWire("HEARTBEAT"))
			Expect(err).To(Equal(dbn.ErrInvalidDBNVersion))
		})
	})

	Context("IsHeartbeat fallback", func() {
		It("returns true when Code is Unset but the message text is HEARTBEAT", func() {
			// Simulates a record where the code byte was never populated (e.g. zero-valued struct).
			rec := dbn.SystemMsg{Code: dbn.SystemCode_Unset}
			copy(rec.Message[:], []byte("HEARTBEAT"))
			Expect(rec.IsHeartbeat()).To(BeTrue())
		})

		It("returns false when the (null-trimmed) text is not HEARTBEAT", func() {
			// Reproduces the broken pre-fix behavior: bytes.Equal on a 303-byte
			// array would never match the 9-byte literal "HEARTBEAT", so this case
			// would have silently returned false; with TrimNullBytes it returns false
			// for genuine non-heartbeat text but TRUE for the heartbeat case above.
			rec := dbn.SystemMsg{Code: dbn.SystemCode_Unset}
			copy(rec.Message[:], []byte("Subscription request 1 for mbp-1 data succeeded"))
			Expect(rec.IsHeartbeat()).To(BeFalse())
		})
	})
})
