// Copyright (c) 2024 Neomantra Corp
//
// Common Message descriptions
//
// intended to allow versionless to the business objeects
//

package dbn

/////////////////////////////////////////////////////////////////////////////
// Version-aware decoders for records that differ across DBN versions.
// These convert V1/V2 records up to the V3 layout (the canonical type).

// decodeErrorMsg decodes a ErrorMsg, upgrading from V1 if needed.
// V1 only has a short error message
func DecodeErrorMsg(metadata *Metadata, body []byte) (*ErrorMsgV2, error) {
	switch metadata.VersionNum {
	case HeaderVersion1:
		var v1 ErrorMsgV1
		if err := v1.Fill_Raw(body[:ErrorMsgV1_Size]); err != nil {
			return nil, err
		}
		var v2 ErrorMsgV2
		copy(v2.Error[:], v1.Error[:])
		return &v2, nil
	case HeaderVersion2, HeaderVersion3:
		var v2 ErrorMsgV2
		if err := v2.Fill_Raw(body[:ErrorMsgV2_Size]); err != nil {
			return nil, err
		}
		return &v2, nil
	default:
		return nil, ErrInvalidDBNVersion
	}
}

// decodeStatMsg decodes a StatMsg, upgrading from V1/V2 if needed.
// V1 and V2 share the same 64-byte layout (int32 Quantity).
// V3 has an 80-byte layout (int64 Quantity).
func DecodeStatMsg(metadata *Metadata, body []byte) (*StatMsgV3, error) {
	switch metadata.VersionNum {
	case HeaderVersion1, HeaderVersion2:
		var v2 StatMsgV2
		if err := v2.Fill_Raw(body[:StatMsgV2_Size]); err != nil {
			return nil, err
		}
		// Upgrade V2 → V3: sign-extend Quantity from int32 to int64
		return &StatMsgV3{
			Header:       v2.Header,
			TsRecv:       v2.TsRecv,
			TsRef:        v2.TsRef,
			Price:        v2.Price,
			Quantity:     int64(v2.Quantity),
			Sequence:     v2.Sequence,
			TsInDelta:    v2.TsInDelta,
			StatType:     v2.StatType,
			ChannelID:    v2.ChannelID,
			UpdateAction: v2.UpdateAction,
			StatFlags:    v2.StatFlags,
		}, nil
	case HeaderVersion3:
		var v3 StatMsgV3
		if err := v3.Fill_Raw(body[:StatMsgV3_Size]); err != nil {
			return nil, err
		}
		return &v3, nil
	default:
		return nil, ErrInvalidDBNVersion
	}
}

// upgradeInstrumentDefMsgV1ToV3 maps a decoded V1 definition to the canonical V3 layout.
// Fields that exist only in V1 (trading_reference_*, md_security_trading_status, settl_price_type) are dropped,
// matching databento/dbn V1→V3 conversion. V3-only leg fields remain zero-valued.
func upgradeInstrumentDefMsgV1ToV3(v1 *InstrumentDefMsgV1) *InstrumentDefMsgV3 {
	v3 := InstrumentDefMsgV3{
		Header:                  v1.Header,
		TsRecv:                  v1.TsRecv,
		MinPriceIncrement:       v1.MinPriceIncrement,
		DisplayFactor:           v1.DisplayFactor,
		Expiration:              v1.Expiration,
		Activation:              v1.Activation,
		HighLimitPrice:          v1.HighLimitPrice,
		LowLimitPrice:           v1.LowLimitPrice,
		MaxPriceVariation:       v1.MaxPriceVariation,
		UnitOfMeasureQty:        v1.UnitOfMeasureQty,
		MinPriceIncrementAmount: v1.MinPriceIncrementAmount,
		PriceRatio:              v1.PriceRatio,
		StrikePrice:             v1.StrikePrice,
		RawInstrumentID:         uint64(v1.RawInstrumentID),
		InstAttribValue:         v1.InstAttribValue,
		UnderlyingID:            v1.UnderlyingID,
		MarketDepthImplied:      v1.MarketDepthImplied,
		MarketDepth:             v1.MarketDepth,
		MarketSegmentID:         v1.MarketSegmentID,
		MaxTradeVol:             v1.MaxTradeVol,
		MinLotSize:              v1.MinLotSize,
		MinLotSizeBlock:         v1.MinLotSizeBlock,
		MinLotSizeRoundLot:      v1.MinLotSizeRoundLot,
		MinTradeVol:             v1.MinTradeVol,
		ContractMultiplier:      v1.ContractMultiplier,
		DecayQuantity:           v1.DecayQuantity,
		OriginalContractSize:    v1.OriginalContractSize,
		ApplID:                  v1.ApplID,
		MaturityYear:            v1.MaturityYear,
		DecayStartDate:          v1.DecayStartDate,
		ChannelID:               v1.ChannelID,
		Currency:                v1.Currency,
		SettlCurrency:           v1.SettlCurrency,
		Secsubtype:              v1.Secsubtype,
		Group:                   v1.Group,
		Exchange:                v1.Exchange,
		Cfi:                     v1.Cfi,
		SecurityType:            v1.SecurityType,
		UnitOfMeasure:           v1.UnitOfMeasure,
		Underlying:              v1.Underlying,
		StrikePriceCurrency:     v1.StrikePriceCurrency,
		InstrumentClass:         v1.InstrumentClass,
		MatchAlgorithm:          v1.MatchAlgorithm,
		MainFraction:            v1.MainFraction,
		PriceDisplayFormat:      v1.PriceDisplayFormat,
		SubFraction:             v1.SubFraction,
		UnderlyingProduct:       v1.UnderlyingProduct,
		SecurityUpdateAction:    v1.SecurityUpdateAction,
		MaturityMonth:           v1.MaturityMonth,
		MaturityDay:             v1.MaturityDay,
		MaturityWeek:            v1.MaturityWeek,
		UserDefinedInstrument:   v1.UserDefinedInstrument,
		ContractMultiplierUnit:  v1.ContractMultiplierUnit,
		FlowScheduleType:        v1.FlowScheduleType,
		TickRule:                v1.TickRule,
	}
	copy(v3.RawSymbol[:], v1.RawSymbol[:])
	copy(v3.Asset[:], v1.Asset[:])
	return &v3
}

// decodeInstrumentDefMsg decodes an InstrumentDefMsg, upgrading from V1/V2 if needed.
// V1 uses 22-byte symbols and a different field order; V2 uses uint32 RawInstrumentID and fields removed in V3.
// V3 has uint64 RawInstrumentID and multi-leg strategy fields.
func DecodeInstrumentDefMsg(metadata *Metadata, body []byte, size int) (*InstrumentDefMsgV3, error) {
	switch metadata.VersionNum {
	case HeaderVersion1:
		if size < InstrumentDefMsgV1_Size {
			return nil, unexpectedBytesError(size, InstrumentDefMsgV1_Size)
		}
		var v1 InstrumentDefMsgV1
		if err := v1.Fill_Raw(body[:size]); err != nil {
			return nil, err
		}
		return upgradeInstrumentDefMsgV1ToV3(&v1), nil
	case HeaderVersion2:
		var v2 InstrumentDefMsgV2
		if err := v2.Fill_Raw(body[:size]); err != nil {
			return nil, err
		}
		// Upgrade V2 → V3: zero-extend RawInstrumentID, drop removed fields, zero-fill leg fields
		v3 := InstrumentDefMsgV3{
			Header:                  v2.Header,
			TsRecv:                  v2.TsRecv,
			MinPriceIncrement:       v2.MinPriceIncrement,
			DisplayFactor:           v2.DisplayFactor,
			Expiration:              v2.Expiration,
			Activation:              v2.Activation,
			HighLimitPrice:          v2.HighLimitPrice,
			LowLimitPrice:           v2.LowLimitPrice,
			MaxPriceVariation:       v2.MaxPriceVariation,
			UnitOfMeasureQty:        v2.UnitOfMeasureQty,
			MinPriceIncrementAmount: v2.MinPriceIncrementAmount,
			PriceRatio:              v2.PriceRatio,
			StrikePrice:             v2.StrikePrice,
			RawInstrumentID:         uint64(v2.RawInstrumentID),
			InstAttribValue:         v2.InstAttribValue,
			UnderlyingID:            v2.UnderlyingID,
			MarketDepthImplied:      v2.MarketDepthImplied,
			MarketDepth:             v2.MarketDepth,
			MarketSegmentID:         v2.MarketSegmentID,
			MaxTradeVol:             v2.MaxTradeVol,
			MinLotSize:              v2.MinLotSize,
			MinLotSizeBlock:         v2.MinLotSizeBlock,
			MinLotSizeRoundLot:      v2.MinLotSizeRoundLot,
			MinTradeVol:             v2.MinTradeVol,
			ContractMultiplier:      v2.ContractMultiplier,
			DecayQuantity:           v2.DecayQuantity,
			OriginalContractSize:    v2.OriginalContractSize,
			ApplID:                  v2.ApplID,
			MaturityYear:            v2.MaturityYear,
			DecayStartDate:          v2.DecayStartDate,
			ChannelID:               v2.ChannelID,
			Currency:                v2.Currency,
			SettlCurrency:           v2.SettlCurrency,
			Secsubtype:              v2.Secsubtype,
			Group:                   v2.Group,
			Exchange:                v2.Exchange,
			Cfi:                     v2.Cfi,
			SecurityType:            v2.SecurityType,
			UnitOfMeasure:           v2.UnitOfMeasure,
			Underlying:              v2.Underlying,
			StrikePriceCurrency:     v2.StrikePriceCurrency,
			InstrumentClass:         v2.InstrumentClass,
			MatchAlgorithm:          v2.MatchAlgorithm,
			MainFraction:            v2.MainFraction,
			PriceDisplayFormat:      v2.PriceDisplayFormat,
			SubFraction:             v2.SubFraction,
			UnderlyingProduct:       v2.UnderlyingProduct,
			SecurityUpdateAction:    v2.SecurityUpdateAction,
			MaturityMonth:           v2.MaturityMonth,
			MaturityDay:             v2.MaturityDay,
			MaturityWeek:            v2.MaturityWeek,
			UserDefinedInstrument:   v2.UserDefinedInstrument,
			ContractMultiplierUnit:  v2.ContractMultiplierUnit,
			FlowScheduleType:        v2.FlowScheduleType,
			TickRule:                v2.TickRule,
			// Leg fields are zero-valued (not present in V2)
		}
		// RawSymbol is the same size in V2 and V3 (71 bytes)
		v3.RawSymbol = v2.RawSymbol
		// Asset: V2 is [7]byte, V3 is [11]byte — copy the smaller into the larger
		copy(v3.Asset[:], v2.Asset[:])
		return &v3, nil
	case HeaderVersion3:
		var v3 InstrumentDefMsgV3
		if err := v3.Fill_Raw(body[:size]); err != nil {
			return nil, err
		}
		return &v3, nil
	default:
		return nil, ErrInvalidDBNVersion
	}
}
