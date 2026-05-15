package dbn

import "testing"

func TestInferSystemCodeFromText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		want SystemCode
	}{
		{"Heartbeat", SystemCode_Heartbeat},
		{"Heartbeat extra", SystemCode_Heartbeat},
		{"End of interval for foo", SystemCode_EndOfInterval},
		{"Subscription request for tbbo data succeeded", SystemCode_SubscriptionAck},
		{"Subscription request foo", SystemCode_Unset},
		{"Warning: slow reading detected", SystemCode_SlowReaderWarning},
		{"Finished tbbo replay", SystemCode_ReplayCompleted},
		{"Finished es replay", SystemCode_ReplayCompleted},
		{"Finished foo", SystemCode_Unset},
		{"random text", SystemCode_Unset},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			t.Parallel()
			if g := inferSystemCodeFromText([]byte(tt.text)); g != tt.want {
				t.Fatalf("inferSystemCodeFromText(%q) = %v want %v", tt.text, g, tt.want)
			}
		})
	}
}

func TestInferSystemCodeV1NoNUL(t *testing.T) {
	var m [SystemMsgV1_MsgSize]byte
	for i := range m {
		m[i] = 'Z'
	}
	msg := "Subscription request for tbbo data succeeded"
	copy(m[:], msg)
	if inferSystemCodeV1(m) != SystemCode_Unset {
		t.Fatalf("expected Unset when no NUL in v1 buffer")
	}
}
