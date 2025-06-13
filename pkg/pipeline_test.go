package pkg

import (
	"testing"
	"time"
)

func TestPipeline_ConnectProcessors(t *testing.T) {
	// 第一个 Processor：窗口内求和
	p1, in, _ := BuildProcessor().
		Window(NewFixedWindows(time.Hour)).
		Trigger(NewCounterTrigger(2)).
		Operator(SumOperator{DataType: OperatorDataTypeInt}).
		Evictor(RecalculateEvictor{}).
		Build()

	// 第二个 Processor：窗口内求和（再次处理）
	p2, _, out := BuildProcessor().
		Window(NewFixedWindows(time.Hour)).
		Trigger(NewCounterTrigger(2)).
		Operator(SumOperator{DataType: OperatorDataTypeInt}).
		Evictor(AccumulateEvictor{}).
		Build()

	pipeline := NewPipeline()

	pipeline.Connect(p1, p2)
	pipeline.Start()
	defer pipeline.Stop()

	// 向第一个 Processor 输入数据
	in <- DU{Key: "user1", Value: 1, EventTime: time.Now()}
	in <- DU{Key: "user1", Value: 2, EventTime: time.Now()}
	in <- DU{Key: "user1", Value: 3, EventTime: time.Now()}
	in <- DU{Key: "user1", Value: 4, EventTime: time.Now()}


	// 由于每两个数据触发一次窗口，第一次窗口输出 1+2=3，第二次 3+4=7
	// 第二个 Processor 也每两个输入触发一次窗口，所以最终输出 3+7=10

	select {
	case result := <-out:
		if result.Value != 10 {
			t.Errorf("expected 10, got %v", result.Value)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for pipeline output")
	}
}
