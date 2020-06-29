package main

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/video-fdmc/godag"
)

type SimpleOp struct {
	data        string
	processTime time.Duration
}

func (o *SimpleOp) Process(ctx context.Context, global interface{}, input ...interface{}) interface{} {
	fmt.Println(time.Now(), "Process begin", o.data, o.processTime, input)
	time.Sleep(o.processTime)
	fmt.Println(time.Now(), "Process done ", o.data, o.processTime, input)
	return nil
}

func main() {
	start := godag.NewStartNode("start")
	op1 := start.AddNext("op1", &SimpleOp{data: "op1_data", processTime: 1 * time.Second})
	op2 := start.AddNext("op2", &SimpleOp{data: "op2_data", processTime: 2 * time.Second})
	op3 := op1.AddNext("op3", &SimpleOp{data: "op3_data", processTime: 3 * time.Second})
	op4 := op3.AddNext("op4", &SimpleOp{data: "op4_data", processTime: 4 * time.Second})
	op2.AddNextNode(op4)

	var dag godag.DAG
	dag.Init(start, nil)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute(context.TODO())
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))
}
