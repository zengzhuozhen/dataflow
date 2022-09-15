package core

import (
	"testing"
	"time"
)

func TestBuildProcessor_Classic_Batch(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewDefaultGlobalWindow()).
		Trigger(NewCounterTrigger(3)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()

	processor.Start()
	input <- Datum{Key: "zzz", Value: 1, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 2, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 3, EventTime: time.Now()}
	sum := <-output
	if sum.Value != 6 {
		t.Errorf("sum want to be %d, got %v", 6, sum.Value)
	}
}

func TestBuildProcessor_Classic_Batch_Accumulate(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewDefaultGlobalWindow()).
		Trigger(NewCounterTrigger(3)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()

	processor.Start()
	input <- Datum{Key: "zzz", Value: 1, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 2, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 3, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 4, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 5, EventTime: time.Now()}

	go func() {
		for {
			sum := <-output
			if sum.Value != 6 && sum.Value != 10 && sum.Value != 15 {
				t.Errorf("sum want to be %d, got %v", 6, sum.Value)
			}
		}
	}()
	time.Sleep(time.Second)

}

func TestBuildProcessor_Classic_Batch_Recalculate(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewDefaultGlobalWindow()).
		Trigger(NewCounterTrigger(3)).
		Operator(SumOperator{}).
		Evictor(RecalculateEvictor{}).
		Build()

	processor.Start()
	input <- Datum{Key: "zzz", Value: 1, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 2, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 3, EventTime: time.Now()}

	sum := <-output
	if sum.Value != 6 {
		t.Errorf("sum want to be %d, got %v", 6, sum.Value)
	}

	input <- Datum{Key: "zzz", Value: 4, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 5, EventTime: time.Now()}
	input <- Datum{Key: "zzz", Value: 6, EventTime: time.Now()}

	sum = <-output
	if sum.Value != 15 {
		t.Errorf("sum want to be %d, got %v", 15, sum.Value)
	}
}

func TestBuildProcessor_Fixed_Windows_Batch(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewFixedWindows(time.Duration(time.Minute) * 2)).
		Trigger(NewCounterTrigger(2)).
		Operator(SumOperator{}).
		Evictor(RecalculateEvictor{}).
		Build()
	processor.Start()

	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2020-01-01 00:00:01")}
	input <- Datum{Key: "zzz", Value: 2, EventTime: helperParseTimeNoError("2020-01-01 00:00:01")}
	input <- Datum{Key: "zzz1", Value: 3, EventTime: helperParseTimeNoError("2020-01-01 00:02:01")}
	input <- Datum{Key: "zzz1", Value: 4, EventTime: helperParseTimeNoError("2020-01-01 00:02:01")}
	input <- Datum{Key: "zzz2", Value: 5, EventTime: helperParseTimeNoError("2020-01-01 00:04:01")}
	input <- Datum{Key: "zzz2", Value: 6, EventTime: helperParseTimeNoError("2020-01-01 00:04:01")}

	sum := <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
}

func TestBuildProcessor_Fixed_Windows_Timer_Trigger_Flow(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewFixedWindows(time.Duration(time.Minute) * 2)).
		Trigger(NewTimerTrigger(time.Second * 3)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()
	processor.Start()

	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2020-01-01 00:00:01")}
	input <- Datum{Key: "zzz1", Value: 3, EventTime: helperParseTimeNoError("2020-01-01 00:02:01")}
	input <- Datum{Key: "zzz2", Value: 5, EventTime: helperParseTimeNoError("2020-01-01 00:04:01")}
	time.Sleep(time.Second * 3)
	sum := <-output
	if sum.Key == "zzz" && sum.Value != 1 || sum.Key == "zzz1" && sum.Value != 3 || sum.Key == "zzz2" && sum.Value != 5 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 1 || sum.Key == "zzz1" && sum.Value != 3 || sum.Key == "zzz2" && sum.Value != 5 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 1 || sum.Key == "zzz1" && sum.Value != 3 || sum.Key == "zzz2" && sum.Value != 5 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	input <- Datum{Key: "zzz", Value: 2, EventTime: helperParseTimeNoError("2020-01-01 00:00:01")}
	input <- Datum{Key: "zzz1", Value: 4, EventTime: helperParseTimeNoError("2020-01-01 00:02:01")}
	input <- Datum{Key: "zzz2", Value: 6, EventTime: helperParseTimeNoError("2020-01-01 00:04:01")}
	time.Sleep(time.Second * 3)
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key == "zzz" && sum.Value != 3 || sum.Key == "zzz1" && sum.Value != 7 || sum.Key == "zzz2" && sum.Value != 11 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
}

func TestBuildProcessor_Slide_Windows_Count_Trigger(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewSlideWindow(time.Duration(time.Minute)*2, time.Minute)). // 2-minute size window, move on per minute
		Trigger(NewCounterTrigger(1)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()
	processor.Start()

	// test this data will assign two window
	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2020-01-01 00:01:01")}
	sum := <-output
	if sum.Key != "zzz" || sum.Value != 1 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
	sum = <-output
	if sum.Key != "zzz" || sum.Value != 1 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
}

func TestBuildProcessor_Slide_Windows_Timer_Trigger(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewSlideWindow(time.Duration(time.Minute)*6, time.Minute)). // 6-minute size window, move on per minute
		Trigger(NewTimerTrigger(3 * time.Second)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()
	processor.Start()

	// this data will assign five window
	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2022-01-01 00:04:01")}
	<-output
	<-output
	<-output
	<-output
	<-output
}

func TestBuildProcessor_Session_Windows_Counter_Trigger(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewSessionWindow(time.Hour)). // 1-hour session window
		Trigger(NewCounterTrigger(3)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()
	processor.Start()
	// although the duration between first data and last data is over 1 hours, it still in one window
	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2022-01-01 00:01:01")}
	input <- Datum{Key: "zzz", Value: 2, EventTime: helperParseTimeNoError("2022-01-01 00:30:01")}
	input <- Datum{Key: "zzz", Value: 3, EventTime: helperParseTimeNoError("2022-01-01 0:59:01")}
	sum := <-output
	if sum.Key != "zzz" || sum.Value != 6 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}
}

func TestBuildProcessor_Session_Windows_Auto_Merge(t *testing.T) {
	processor, input, output := BuildProcessor().
		Window(NewSessionWindow(time.Minute * 30)).
		Trigger(NewCounterTrigger(3)).
		Operator(SumOperator{}).
		Evictor(AccumulateEvictor{}).
		Build()
	processor.Start()

	// first data drop in the first window
	input <- Datum{Key: "zzz", Value: 1, EventTime: helperParseTimeNoError("2022-01-01 00:01:01")}
	// second data drop in other window
	input <- Datum{Key: "zzz", Value: 2, EventTime: helperParseTimeNoError("2022-01-01 01:01:01")}
	// third data drop in a new window between first and second,and they will auto merge become bigger one
	input <- Datum{Key: "zzz", Value: 3, EventTime: helperParseTimeNoError("2022-01-01 00:40:01")}

	sum := <-output
	if sum.Key != "zzz" || sum.Value != 6 {
		t.Errorf("sum.Key:%s,sum.Value:%d", sum.Key, sum.Value)
	}

}

func helperParseTimeNoError(timeValue string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05", timeValue)
	return t
}
