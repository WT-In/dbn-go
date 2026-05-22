package dbn_test

import (
	"os"

	"github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/valyala/fastjson"
)

var _ = Describe("JsonScanner", func() {
	Context("json files", func() {
		It("should read a JSON metadata test file correctly", func() {
			file, err := os.Open("./tests/data/test_data.ohlcv-1s.meta.json")
			Expect(err).To(BeNil())
			defer file.Close()

			// TODO: slurp meta
		})
		It("should read a JSON test file correctly", func() {
			file, err := os.Open("./tests/data/test_data.ohlcv-1s.json")
			Expect(err).To(BeNil())
			defer file.Close()

			records, err := dbn.ReadJsonToSlice[dbn.OhlcvMsg](file)
			Expect(err).To(BeNil())
			Expect(len(records)).To(Equal(2))

			// dbn -J ./tests/data/test_data.ohlcv-1s.dbn
			// {"hd":{"ts_event":"1609160400000000000","rtype":32,"publisher_id":1,"instrument_id":5482},"open":"372025000000000","high":"372050000000000","low":"372025000000000","close":"372050000000000","volume":"57"}
			// {"hd":{"ts_event":"1609160401000000000","rtype":32,"publisher_id":1,"instrument_id":5482},"open":"372050000000000","high":"372050000000000","low":"372050000000000","close":"372050000000000","volume":"13"}

			r0, r0h := records[0], records[0].Header
			Expect(r0h.TsEvent).To(Equal(uint64(1609160400000000000)))
			Expect(r0h.RType).To(Equal(dbn.RType(32)))
			Expect(r0h.PublisherID).To(Equal(uint16(1)))
			Expect(r0h.InstrumentID).To(Equal(uint32(5482)))
			Expect(r0.Open).To(Equal(int64(372025000000000)))
			Expect(r0.High).To(Equal(int64(372050000000000)))
			Expect(r0.Low).To(Equal(int64(372025000000000)))
			Expect(r0.Close).To(Equal(int64(372050000000000)))
			Expect(r0.Volume).To(Equal(uint64(57)))

			r1, r1h := records[1], records[1].Header
			Expect(r1h.TsEvent).To(Equal(uint64(1609160401000000000)))
			Expect(r1h.RType).To(Equal(dbn.RType(32)))
			Expect(r1h.PublisherID).To(Equal(uint16(1)))
			Expect(r1h.InstrumentID).To(Equal(uint32(5482)))
			Expect(r1.Open).To(Equal(int64(372050000000000)))
			Expect(r1.High).To(Equal(int64(372050000000000)))
			Expect(r1.Low).To(Equal(int64(372050000000000)))
			Expect(r1.Close).To(Equal(int64(372050000000000)))
			Expect(r1.Volume).To(Equal(uint64(13)))
		})
	})

	Context("SystemMsg JSON without code", func() {
		It("Fill_Json should infer SubscriptionAck from message body", func() {
			var p fastjson.Parser
			line := `{"hd":{"ts_event":1,"rtype":23,"publisher_id":0,"instrument_id":0},"msg":"Subscription request for tbbo data succeeded"}`
			val, err := p.Parse(line)
			Expect(err).To(BeNil())
			var hdr dbn.RHeader
			Expect(hdr.Fill_Json(val.Get("hd"))).To(BeNil())
			var msg dbn.SystemMsg
			Expect(msg.Fill_Json(val, &hdr)).To(BeNil())
			Expect(msg.Code == dbn.SystemCode_SubscriptionAck).To(BeTrue())
		})
	})

	Context("ErrorMsg JSON without code", func() {
		It("Fill_Json should infer SymbolResolutionFailed from error body", func() {
			var p fastjson.Parser
			line := `{"hd":{"ts_event":1,"rtype":29,"publisher_id":0,"instrument_id":0},"err":"Failed to resolve symbol 6/100: FSBAL.c.0"}`
			val, err := p.Parse(line)
			Expect(err).To(BeNil())
			var hdr dbn.RHeader
			Expect(hdr.Fill_Json(val.Get("hd"))).To(BeNil())
			var msg dbn.ErrorMsg
			Expect(msg.Fill_Json(val, &hdr)).To(BeNil())
			Expect(msg.Code).To(Equal(dbn.ErrorCode_SymbolResolutionFailed))
		})
	})
})
