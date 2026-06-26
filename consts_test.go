// Copyright (c) 2024 Neomantra Corp

package dbn_test

import (
	dbn "github.com/WT-In/dbn-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ErrorCode", func() {
	Context("IsFatal", func() {
		It("reports fatal codes that close the session", func() {
			for _, code := range []dbn.ErrorCode{
				dbn.ErrorCode_AuthFailed,
				dbn.ErrorCode_ApiKeyDeactivated,
				dbn.ErrorCode_ConnectionLimitExceeded,
				dbn.ErrorCode_InvalidSubscription,
				dbn.ErrorCode_InternalError,
			} {
				Expect(code.IsFatal()).To(BeTrue(), code.String())
			}
		})

		It("reports non-fatal and unset codes as not fatal", func() {
			for _, code := range []dbn.ErrorCode{
				dbn.ErrorCode_SymbolResolutionFailed,
				dbn.ErrorCode_SkippedRecordsAfterSlowReading,
				dbn.ErrorCode_Unset,
			} {
				Expect(code.IsFatal()).To(BeFalse(), code.String())
			}
		})
	})
})
