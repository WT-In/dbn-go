package dbn_test

import (
	"bytes"
	"encoding/binary"

	dbn "github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type systemCapturingVisitor struct {
	dbn.NullVisitor
	Systems []dbn.SystemMsg
}

func (v *systemCapturingVisitor) OnSystemMsg(r *dbn.SystemMsg) error {
	v.Systems = append(v.Systems, *r)
	return nil
}

func makeSystemMsgStream(version uint8, messages ...string) *bytes.Reader {
	var stream bytes.Buffer
	metadata := dbn.Metadata{
		VersionNum: version,
		Dataset:    "GLBX.MDP3",
		Schema:     dbn.Schema_Mbp1,
	}
	Expect(metadata.Write(&stream)).To(Succeed())

	recordSize := dbn.SystemMsg_Size
	msgSize := dbn.SystemMsg_MsgSize
	if version == dbn.HeaderVersion1 {
		recordSize = dbn.SystemMsgV1_Size
		msgSize = dbn.SystemMsgV1_MsgSize
	}
	for i, msg := range messages {
		record := make([]byte, recordSize)
		record[0] = byte(recordSize / 4)
		record[1] = byte(dbn.RType_System)
		binary.LittleEndian.PutUint64(record[8:16], uint64(100+i))
		copy(record[dbn.RHeader_Size:dbn.RHeader_Size+msgSize], msg)
		stream.Write(record)
	}
	return bytes.NewReader(stream.Bytes())
}

var _ = Describe("SystemMsg", func() {
	Context("version-aware decoding", func() {
		It("should upgrade V1 system message codes via Visit", func() {
			scanner := dbn.NewDbnScanner(makeSystemMsgStream(
				dbn.HeaderVersion1,
				"Heartbeat",
				"Subscription request 1 succeeded",
				"Warning: slow reading; client is behind",
				"Finished GLBX.MDP3 replay",
				"End of interval for 2024-01-01T00:00:00Z",
				"Something else",
			))
			visitor := &systemCapturingVisitor{}
			Expect(visitAll(scanner, visitor)).To(Succeed())
			Expect(visitor.Systems).To(HaveLen(6))

			Expect(visitor.Systems[0].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_Heartbeat)))
			Expect(visitor.Systems[0].IsHeartbeat()).To(BeTrue())
			Expect(visitor.Systems[1].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_SubscriptionAck)))
			Expect(visitor.Systems[1].IsHeartbeat()).To(BeFalse())
			Expect(visitor.Systems[2].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_SlowReaderWarning)))
			Expect(visitor.Systems[3].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_ReplayCompleted)))
			Expect(visitor.Systems[4].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_EndOfInterval)))
			Expect(visitor.Systems[5].Code).To(Equal(dbn.SystemCode(dbn.SystemCode_Unset)))
		})

		It("should decode V1 system message codes via DecodeSystemMsg", func() {
			scanner := dbn.NewDbnScanner(makeSystemMsgStream(
				dbn.HeaderVersion1,
				"Subscription request 1 succeeded",
			))
			Expect(scanner.Next()).To(BeTrue())

			record, err := scanner.DecodeSystemMsg()
			Expect(err).To(BeNil())
			Expect(record.Code).To(Equal(dbn.SystemCode(dbn.SystemCode_SubscriptionAck)))
			Expect(record.IsHeartbeat()).To(BeFalse())
			Expect(dbn.TrimNullBytes(record.Message[:])).To(Equal("Subscription request 1 succeeded"))
		})
	})

	Context("heartbeat detection", func() {
		It("should only use the message fallback when code is unset", func() {
			var heartbeat dbn.SystemMsg
			heartbeat.Code = dbn.SystemCode_Unset
			copy(heartbeat.Message[:], "Heartbeat")
			Expect(heartbeat.IsHeartbeat()).To(BeTrue())

			var subscriptionAck dbn.SystemMsg
			subscriptionAck.Code = dbn.SystemCode_Unset
			copy(subscriptionAck.Message[:], "Subscription request 1 succeeded")
			Expect(subscriptionAck.IsHeartbeat()).To(BeFalse())

			var codedAck dbn.SystemMsg
			codedAck.Code = dbn.SystemCode_SubscriptionAck
			copy(codedAck.Message[:], "Heartbeat")
			Expect(codedAck.IsHeartbeat()).To(BeFalse())
		})
	})
})
