package pkg

import (
	"testing"
)

func TestSumOperator_Operate(t *testing.T) {
	type fields struct {
		DataType int32
	}
	type args struct {
		DUs []DU
	}
	var tests = []struct {
		name   string
		fields fields
		args   args
		want   DU
	}{
		{
			name: "DataType(int)",
			fields: fields{
				DataType: OperatorDataTypeInt,
			},
			args: args{
				DUs: []DU{
					{Key: "zzz", Value: 1},
					{Key: "zzz", Value: 2},
					{Key: "zzz", Value: 3},
				},
			},
			want: DU{
				Key:   "zzz",
				Value: 6,
			},
		},
		{
			name: "DataType(float)",
			fields: fields{
				DataType: OperatorDataTypeFloat,
			},
			args: args{
				DUs: []DU{
					{Key: "zzz", Value: 1.20},
					{Key: "zzz", Value: 2.20},
					{Key: "zzz", Value: 3.20},
				},
			},
			want: DU{
				Key:   "zzz",
				Value: 6.6000000000000005,
			},
		},
		{
			name: "DataType(string)",
			fields: fields{
				DataType: OperatorDataTypeString,
			},
			args: args{
				DUs: []DU{
					{Key: "zzz", Value: "hello "},
					{Key: "zzz", Value: "world "},
					{Key: "zzz", Value: "zzz"},
				},
			},
			want: DU{
				Key:   "zzz",
				Value: "hello world zzz",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SumOperator{
				DataType: tt.fields.DataType,
			}
			if got := s.Operate(tt.args.DUs); !(got.Key == tt.want.Key && got.Value == tt.want.Value) {
				t.Errorf("Operate() = %v, want %v", got, tt.want)
			}
		})
	}
}
