package main

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/yjzhuang/dag"
)

type StateOp struct {
	data        string
	processTime time.Duration
}

func (o *StateOp) Process(ctx context.Context, input ...interface{}) interface{} {
	id := "start"
	idVal := ctx.Value(dag.StateKey("id"))
	if idVal != nil {
		id = idVal.(string)
	}
	
	fmt.Println(time.Now(), "Process begin", o.data, o.processTime, input)
	time.Sleep(o.processTime)
	fmt.Println(time.Now(), "Process done ", o.data, o.processTime, input)
	output := ""
	for _, param := range input {
		if param != nil {
			output = output + "(" + param.(string) + ")"
		}
	}
	output = output +  "[" + id + "> " + o.data + "]"
	return output
}

func main() {
	start := dag.NewStartNode("start")
	op1 := start.AddNext("op1", &StateOp{data: "op1_data", processTime: 1 * time.Second})
	op2 := start.AddNext("op2", &StateOp{data: "op2_data", processTime: 2 * time.Second})
	op3 := op1.AddNext("op3", &StateOp{data: "op3_data", processTime: 3 * time.Second})
	op4 := op3.AddNext("op4", &StateOp{data: "op4_data", processTime: 4 * time.Second})
	op2.AddNextNode(op4)

	var dag dag.DAG
	dag.Init(start, nil)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute()
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))
}
