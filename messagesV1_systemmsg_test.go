package dbn_test

import (
	"bytes"
	"encoding/binary"

	dbn "github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func buildV1SystemWire(msg string) []byte {
	b := make([]byte, dbn.SystemMsgV1_Size)
	b[0] = uint8(dbn.SystemMsgV1_Size / 4)
	b[1] = byte(dbn.RType_System)
	binary.LittleEndian.PutUint16(b[2:4], 7)
	binary.LittleEndian.PutUint32(b[4:8], 12345)
	binary.LittleEndian.PutUint64(b[8:16], 999)
	copy(b[16:], msg)
	b[16+len(msg)] = 0
	return b
}

var _ = Describe("SystemMsg V1 and DecodeSystemMsg", func() {
	It("should round-trip SystemMsgV1.Fill_Raw", func() {
		raw := buildV1SystemWire("Finished ES replay")
		var v1 dbn.SystemMsgV1
		Expect(v1.Fill_Raw(raw)).To(Succeed())
		Expect(v1.Header.RType).To(Equal(dbn.RType_System))
		Expect(v1.Header.Length).To(Equal(uint8(dbn.SystemMsgV1_Size / 4)))
		Expect(bytes.TrimRight(v1.Msg[:], "\x00")).To(Equal([]byte("Finished ES replay")))
	})

	It("DecodeSystemMsg should upgrade v1 subscription ack", func() {
		md := &dbn.Metadata{VersionNum: dbn.HeaderVersion1}
		raw := buildV1SystemWire("Subscription request for tbbo data succeeded")
		out, err := dbn.DecodeSystemMsg(md, raw)
		Expect(err).To(BeNil())
		Expect(out.Code == dbn.SystemCode_SubscriptionAck).To(BeTrue())
		Expect(out.Header.Length).To(Equal(uint8(dbn.SystemMsg_Size / 4)))
		Expect(bytes.TrimRight(out.Message[:], "\x00")).To(Equal([]byte("Subscription request for tbbo data succeeded")))
	})

	It("DecodeSystemMsg should upgrade v1 replay completed", func() {
		md := &dbn.Metadata{VersionNum: dbn.HeaderVersion1}
		raw := buildV1SystemWire("Finished tbbo replay")
		out, err := dbn.DecodeSystemMsg(md, raw)
		Expect(err).To(BeNil())
		Expect(out.Code == dbn.SystemCode_ReplayCompleted).To(BeTrue())
	})

	It("DecodeSystemMsg v2 path should preserve explicit code", func() {
		md := &dbn.Metadata{VersionNum: dbn.HeaderVersion2}
		raw := make([]byte, dbn.SystemMsg_Size)
		raw[0] = uint8(dbn.SystemMsg_Size / 4)
		raw[1] = byte(dbn.RType_System)
		binary.LittleEndian.PutUint64(raw[8:16], 1)
		copy(raw[16:16+len("Hi")], "Hi")
		raw[16+dbn.SystemMsg_MsgSize] = byte(dbn.SystemCode_SlowReaderWarning)
		out, err := dbn.DecodeSystemMsg(md, raw)
		Expect(err).To(BeNil())
		Expect(out.Code == dbn.SystemCode_SlowReaderWarning).To(BeTrue())
	})
})
