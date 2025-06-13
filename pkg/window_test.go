package pkg

import (
	"reflect"
	"testing"
	"time"
)

func Test_findRegularTime(t *testing.T) {
	type args struct {
		eventTime time.Time
		size      time.Duration
	}
	tests := []struct {
		name      string
		args      args
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name: "1小时",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      time.Hour,
			},
			wantStart: helperParseTimeNoError("2022-01-01 08:00:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 09:00:00"),
		},
		{
			name: "2小时",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      2 * time.Hour,
			},
			wantStart: helperParseTimeNoError("2022-01-01 08:00:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 10:00:00"),
		},
		{
			name: "3小时",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      3 * time.Hour,
			},
			wantStart: helperParseTimeNoError("2022-01-01 06:00:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 09:00:00"),
		},
		{
			name: "1分钟",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      1 * time.Minute,
			},
			wantStart: helperParseTimeNoError("2022-01-01 08:05:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 08:06:00"),
		},
		{
			name: "10分钟",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      10 * time.Minute,
			},
			wantStart: helperParseTimeNoError("2022-01-01 08:00:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 08:10:00"),
		},
		{
			name: "30分钟",
			args: args{
				eventTime: helperParseTimeNoError("2022-01-01 08:05:03"),
				size:      30 * time.Minute,
			},
			wantStart: helperParseTimeNoError("2022-01-01 08:00:00"),
			wantEnd:   helperParseTimeNoError("2022-01-01 08:30:00"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := findStartAndEndTime(tt.args.eventTime, tt.args.size, 0)
			if !reflect.DeepEqual(gotStart, tt.wantStart) {
				t.Errorf("findStartAndEndTime() gotStart = %v, want %v", gotStart, tt.wantStart)
			}
			if !reflect.DeepEqual(gotEnd, tt.wantEnd) {
				t.Errorf("findStartAndEndTime() gotEnd = %v, want %v", gotEnd, tt.wantEnd)
			}
		})
	}
}
