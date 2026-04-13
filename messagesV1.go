// Copyright (c) 2024 Neomantra Corp
//
// DBN Version 1 Message Structs
//
// These structs represent the DBN version 1 binary layout.
// V1 uses 22-byte symbol strings and has different field layouts than V2.
//
// Adapted from Databento's DBN:
//   https://github.com/databento/dbn

package dbn

import (
	"encoding/binary"

	"github.com/valyala/fastjson"
)

///////////////////////////////////////////////////////////////////////////////

// ErrorMsgV1 is the DBN version Protocol Error Message
type ErrorMsgV1 struct {
	Header RHeader                  `json:"hd" csv:"hd"`   // The common header.
	Error  [ErrorMsgV1_ErrSize]byte `json:"err" csv:"err"` // The error message.
}

const ErrorMsgV1_ErrSize = 64
const ErrorMsgV1_Size = RHeader_Size + ErrorMsgV1_ErrSize

func (*ErrorMsgV1) RType() RType {
	return RType_Error
}

func (*ErrorMsgV1) RSize() uint16 {
	return ErrorMsgV1_Size
}

func (r *ErrorMsgV1) Fill_Raw(b []byte) error {
	if len(b) < ErrorMsgV1_Size {
		return unexpectedBytesError(len(b), ErrorMsgV1_Size)
	}
	err := r.Header.Fill_Raw(b[0:RHeader_Size])
	if err != nil {
		return err
	}
	body := b[RHeader_Size:] // slice of just the body
	copy(r.Error[:], body[:ErrorMsgV1_ErrSize])
	return nil
}

func (r *ErrorMsgV1) Fill_Json(val *fastjson.Value, header *RHeader) error {
	r.Header = *header
	copy(r.Error[:], val.GetStringBytes("err"))
	return nil
}

///////////////////////////////////////////////////////////////////////////////

// SymbolMappingMsgV1 is the DBN version 1 layout.
// V1 does not have StypeIn/StypeOut fields in the binary format.
type SymbolMappingMsgV1 struct {
	Header         RHeader `json:"hd" csv:"hd"`
	StypeIn        SType   `json:"stype_in" csv:"stype_in"` // Always SType_RawSymbol in V1
	StypeInSymbol  string  `json:"stype_in_symbol" csv:"stype_in_symbol"`
	StypeOut       SType   `json:"stype_out" csv:"stype_out"` // Always SType_RawSymbol in V1
	StypeOutSymbol string  `json:"stype_out_symbol" csv:"stype_out_symbol"`
	StartTs        uint64  `json:"start_ts" csv:"start_ts"`
	EndTs          uint64  `json:"end_ts" csv:"end_ts"`
}

func (*SymbolMappingMsgV1) RType() RType {
	return RType_SymbolMapping
}

const SymbolMappingMsgV1_Size = RHeader_Size + 16 + (2 * MetadataV1_SymbolCstrLen)

func (*SymbolMappingMsgV1) RSize() uint16 {
	return SymbolMappingMsgV1_Size
}

func (r *SymbolMappingMsgV1) Fill_Raw(b []byte) error {
	rsize := r.RSize()
	if len(b) < int(rsize) {
		return unexpectedBytesError(len(b), int(rsize))
	}
	err := r.Header.Fill_Raw(b[0:RHeader_Size])
	if err != nil {
		return err
	}
	body := b[RHeader_Size:]
	// V1 has no StypeIn/StypeOut bytes, just the two symbols
	r.StypeIn = SType_RawSymbol
	r.StypeInSymbol = TrimNullBytes(body[0:MetadataV1_SymbolCstrLen])
	r.StypeOut = SType_RawSymbol
	r.StypeOutSymbol = TrimNullBytes(body[MetadataV1_SymbolCstrLen : 2*MetadataV1_SymbolCstrLen])
	pos := 2 * MetadataV1_SymbolCstrLen
	r.StartTs = binary.LittleEndian.Uint64(body[pos : pos+8])
	r.EndTs = binary.LittleEndian.Uint64(body[pos+8 : pos+16])
	return nil
}

func (r *SymbolMappingMsgV1) Fill_Json(val *fastjson.Value, header *RHeader) error {
	r.Header = *header
	r.StypeIn = SType(val.GetUint("stype_in"))
	r.StypeInSymbol = string(val.GetStringBytes("stype_in_symbol"))
	r.StypeOut = SType(val.GetUint("stype_out"))
	r.StypeOutSymbol = string(val.GetStringBytes("stype_out_symbol"))
	r.StartTs = val.GetUint64("start_ts")
	r.EndTs = val.GetUint64("end_ts")
	return nil
}

///////////////////////////////////////////////////////////////////////////////

// InstrumentDefMsgV1 is the DBN version 1 instrument definition layout.
// V1 uses 22-byte raw_symbol, uint32 raw_instrument_id, and places strike_price after the string block.
// Total record size is 360 bytes (including RHeader). See databento/dbn InstrumentDefMsgV1 in compat.rs.
type InstrumentDefMsgV1 struct {
	Header                  RHeader                        `json:"hd" csv:"hd"`
	TsRecv                  uint64                         `json:"ts_recv" csv:"ts_recv"`
	MinPriceIncrement       int64                          `json:"min_price_increment" csv:"min_price_increment"`
	DisplayFactor           int64                          `json:"display_factor" csv:"display_factor"`
	Expiration              uint64                         `json:"expiration" csv:"expiration"`
	Activation              uint64                         `json:"activation" csv:"activation"`
	HighLimitPrice          int64                          `json:"high_limit_price" csv:"high_limit_price"`
	LowLimitPrice           int64                          `json:"low_limit_price" csv:"low_limit_price"`
	MaxPriceVariation       int64                          `json:"max_price_variation" csv:"max_price_variation"`
	TradingReferencePrice   int64                          `json:"trading_reference_price" csv:"trading_reference_price"` // Not present on V3; dropped on upgrade
	UnitOfMeasureQty        int64                          `json:"unit_of_measure_qty" csv:"unit_of_measure_qty"`
	MinPriceIncrementAmount int64                          `json:"min_price_increment_amount" csv:"min_price_increment_amount"`
	PriceRatio              int64                          `json:"price_ratio" csv:"price_ratio"`
	InstAttribValue         int32                          `json:"inst_attrib_value" csv:"inst_attrib_value"`
	UnderlyingID            uint32                         `json:"underlying_id" csv:"underlying_id"`
	RawInstrumentID         uint32                         `json:"raw_instrument_id" csv:"raw_instrument_id"`
	MarketDepthImplied      int32                          `json:"market_depth_implied" csv:"market_depth_implied"`
	MarketDepth             int32                          `json:"market_depth" csv:"market_depth"`
	MarketSegmentID         uint32                         `json:"market_segment_id" csv:"market_segment_id"`
	MaxTradeVol             uint32                         `json:"max_trade_vol" csv:"max_trade_vol"`
	MinLotSize              int32                          `json:"min_lot_size" csv:"min_lot_size"`
	MinLotSizeBlock         int32                          `json:"min_lot_size_block" csv:"min_lot_size_block"`
	MinLotSizeRoundLot      int32                          `json:"min_lot_size_round_lot" csv:"min_lot_size_round_lot"`
	MinTradeVol             uint32                         `json:"min_trade_vol" csv:"min_trade_vol"`
	ContractMultiplier      int32                          `json:"contract_multiplier" csv:"contract_multiplier"`
	DecayQuantity           int32                          `json:"decay_quantity" csv:"decay_quantity"`
	OriginalContractSize    int32                          `json:"original_contract_size" csv:"original_contract_size"`
	TradingReferenceDate    uint16                         `json:"trading_reference_date" csv:"trading_reference_date"` // Not present on V3; dropped on upgrade
	ApplID                  int16                          `json:"appl_id" csv:"appl_id"`
	MaturityYear            uint16                         `json:"maturity_year" csv:"maturity_year"`
	DecayStartDate          uint16                         `json:"decay_start_date" csv:"decay_start_date"`
	ChannelID               uint16                         `json:"channel_id" csv:"channel_id"`
	Currency                [4]byte                        `json:"currency" csv:"currency"`
	SettlCurrency           [4]byte                        `json:"settl_currency" csv:"settl_currency"`
	Secsubtype              [6]byte                        `json:"secsubtype" csv:"secsubtype"`
	RawSymbol               [MetadataV1_SymbolCstrLen]byte `json:"raw_symbol" csv:"raw_symbol"`
	Group                   [21]byte                       `json:"group" csv:"group"`
	Exchange                [5]byte                        `json:"exchange" csv:"exchange"`
	Asset                   [MetadataV1_AssetCStrLen]byte  `json:"asset" csv:"asset"`
	Cfi                     [7]byte                        `json:"cfi" csv:"cfi"`
	SecurityType            [7]byte                        `json:"security_type" csv:"security_type"`
	UnitOfMeasure           [31]byte                       `json:"unit_of_measure" csv:"unit_of_measure"`
	Underlying              [21]byte                       `json:"underlying" csv:"underlying"`
	StrikePriceCurrency     [4]byte                        `json:"strike_price_currency" csv:"strike_price_currency"`
	InstrumentClass         byte                           `json:"instrument_class" csv:"instrument_class"`
	StrikePrice             int64                          `json:"strike_price" csv:"strike_price"`
	MatchAlgorithm          byte                           `json:"match_algorithm" csv:"match_algorithm"`
	MdSecurityTradingStatus uint8                          `json:"md_security_trading_status" csv:"md_security_trading_status"` // Not present on V3; dropped on upgrade
	MainFraction            uint8                          `json:"main_fraction" csv:"main_fraction"`
	PriceDisplayFormat      uint8                          `json:"price_display_format" csv:"price_display_format"`
	SettlPrice_type         uint8                          `json:"settl_price_type" csv:"settl_price_type"` // Not present on V3; dropped on upgrade
	SubFraction             uint8                          `json:"sub_fraction" csv:"sub_fraction"`
	UnderlyingProduct       uint8                          `json:"underlying_product" csv:"underlying_product"`
	SecurityUpdateAction    byte                           `json:"security_update_action" csv:"security_update_action"`
	MaturityMonth           uint8                          `json:"maturity_month" csv:"maturity_month"`
	MaturityDay             uint8                          `json:"maturity_day" csv:"maturity_day"`
	MaturityWeek            uint8                          `json:"maturity_week" csv:"maturity_week"`
	UserDefinedInstrument   UserDefinedInstrument          `json:"user_defined_instrument" csv:"user_defined_instrument"`
	ContractMultiplierUnit  int8                           `json:"contract_multiplier_unit" csv:"contract_multiplier_unit"`
	FlowScheduleType        int8                           `json:"flow_schedule_type" csv:"flow_schedule_type"`
	TickRule                uint8                          `json:"tick_rule" csv:"tick_rule"`
}

const InstrumentDefMsgV1_Size = 360

func (*InstrumentDefMsgV1) RType() RType {
	return RType_InstrumentDef
}

func (*InstrumentDefMsgV1) RSize() uint16 {
	return InstrumentDefMsgV1_Size
}

// Fill_Raw parses the V1 wire layout (22-byte symbols, strike after strings, u32 raw_instrument_id).
func (r *InstrumentDefMsgV1) Fill_Raw(b []byte) error {
	if len(b) < InstrumentDefMsgV1_Size {
		return unexpectedBytesError(len(b), InstrumentDefMsgV1_Size)
	}
	if err := r.Header.Fill_Raw(b[0:RHeader_Size]); err != nil {
		return err
	}
	body := b[RHeader_Size:]
	r.TsRecv = binary.LittleEndian.Uint64(body[0:8])
	r.MinPriceIncrement = int64(binary.LittleEndian.Uint64(body[8:16]))
	r.DisplayFactor = int64(binary.LittleEndian.Uint64(body[16:24]))
	r.Expiration = binary.LittleEndian.Uint64(body[24:32])
	r.Activation = binary.LittleEndian.Uint64(body[32:40])
	r.HighLimitPrice = int64(binary.LittleEndian.Uint64(body[40:48]))
	r.LowLimitPrice = int64(binary.LittleEndian.Uint64(body[48:56]))
	r.MaxPriceVariation = int64(binary.LittleEndian.Uint64(body[56:64]))
	r.TradingReferencePrice = int64(binary.LittleEndian.Uint64(body[64:72]))
	r.UnitOfMeasureQty = int64(binary.LittleEndian.Uint64(body[72:80]))
	r.MinPriceIncrementAmount = int64(binary.LittleEndian.Uint64(body[80:88]))
	r.PriceRatio = int64(binary.LittleEndian.Uint64(body[88:96]))
	r.InstAttribValue = int32(binary.LittleEndian.Uint32(body[96:100]))
	r.UnderlyingID = binary.LittleEndian.Uint32(body[100:104])
	r.RawInstrumentID = binary.LittleEndian.Uint32(body[104:108])
	r.MarketDepthImplied = int32(binary.LittleEndian.Uint32(body[108:112]))
	r.MarketDepth = int32(binary.LittleEndian.Uint32(body[112:116]))
	r.MarketSegmentID = binary.LittleEndian.Uint32(body[116:120])
	r.MaxTradeVol = binary.LittleEndian.Uint32(body[120:124])
	r.MinLotSize = int32(binary.LittleEndian.Uint32(body[124:128]))
	r.MinLotSizeBlock = int32(binary.LittleEndian.Uint32(body[128:132]))
	r.MinLotSizeRoundLot = int32(binary.LittleEndian.Uint32(body[132:136]))
	r.MinTradeVol = binary.LittleEndian.Uint32(body[136:140])
	// _reserved2 [4] at 140:144
	r.ContractMultiplier = int32(binary.LittleEndian.Uint32(body[144:148]))
	r.DecayQuantity = int32(binary.LittleEndian.Uint32(body[148:152]))
	r.OriginalContractSize = int32(binary.LittleEndian.Uint32(body[152:156]))
	// _reserved3 [4] at 156:160
	r.TradingReferenceDate = binary.LittleEndian.Uint16(body[160:162])
	r.ApplID = int16(binary.LittleEndian.Uint16(body[162:164]))
	r.MaturityYear = binary.LittleEndian.Uint16(body[164:166])
	r.DecayStartDate = binary.LittleEndian.Uint16(body[166:168])
	r.ChannelID = binary.LittleEndian.Uint16(body[168:170])
	copy(r.Currency[:], body[170:174])
	copy(r.SettlCurrency[:], body[174:178])
	copy(r.Secsubtype[:], body[178:184])
	symbolEnd := 184 + int(MetadataV1_SymbolCstrLen)
	copy(r.RawSymbol[:], body[184:symbolEnd])
	copy(r.Group[:], body[symbolEnd:symbolEnd+21])
	copy(r.Exchange[:], body[symbolEnd+21:symbolEnd+26])
	copy(r.Asset[:], body[symbolEnd+26:symbolEnd+33])
	copy(r.Cfi[:], body[symbolEnd+33:symbolEnd+40])
	copy(r.SecurityType[:], body[symbolEnd+40:symbolEnd+47])
	copy(r.UnitOfMeasure[:], body[symbolEnd+47:symbolEnd+78])
	copy(r.Underlying[:], body[symbolEnd+78:symbolEnd+99])
	copy(r.StrikePriceCurrency[:], body[symbolEnd+99:symbolEnd+103])
	pos := symbolEnd + 103
	r.InstrumentClass = body[pos]
	pos += 3 // instrument_class + _reserved4 [2]
	r.StrikePrice = int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
	pos += 8
	pos += 6 // _reserved5 [6]
	r.MatchAlgorithm = body[pos]
	r.MdSecurityTradingStatus = body[pos+1]
	r.MainFraction = body[pos+2]
	r.PriceDisplayFormat = body[pos+3]
	r.SettlPrice_type = body[pos+4]
	r.SubFraction = body[pos+5]
	r.UnderlyingProduct = body[pos+6]
	r.SecurityUpdateAction = body[pos+7]
	r.MaturityMonth = body[pos+8]
	r.MaturityDay = body[pos+9]
	r.MaturityWeek = body[pos+10]
	r.UserDefinedInstrument = UserDefinedInstrument(body[pos+11])
	r.ContractMultiplierUnit = int8(body[pos+12])
	r.FlowScheduleType = int8(body[pos+13])
	r.TickRule = body[pos+14]
	// _dummy [3] at end — ignored
	return nil
}

///////////////////////////////////////////////////////////////////////////////
