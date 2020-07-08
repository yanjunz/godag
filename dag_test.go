package godag


import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)


type SimpleOp struct {
	data        string
	processTime time.Duration
	checker func (nodeID string)
}

func (o *SimpleOp) Process(ctx context.Context, global interface{}, input ...interface{}) interface{} {
	if o.checker != nil {
		o.checker(ctx.Value(StateKey(NodeID)).(string))
	}
	fmt.Println(time.Now(), "Process begin", o.data, o.processTime, input)
	time.Sleep(o.processTime)
	fmt.Println(time.Now(), "Process done ", o.data, o.processTime, input)
	return o.data
}

func TestSimple(t *testing.T) {
	fmt.Println("TestSimple...")
	nodeMap := make(map[string]*Node)
	checker := func (nodeID string) {
		node := nodeMap[nodeID]
		assert.Equal(t, 0, node.indegree)	// check the indegree of running node is 0
	}
	/**
           |-> op1 -> op3 -> |
    start->|                 |-> op4
           |-> op2 --------->|
	**/
	start := NewStartNode("start")
	op1 := start.AddNext("op1", &SimpleOp{data: "op1_data", processTime: 1 * time.Second, checker: checker})
	op2 := start.AddNext("op2", &SimpleOp{data: "op2_data", processTime: 2 * time.Second, checker: checker})
	op3 := op1.AddNext("op3", &SimpleOp{data: "op3_data", processTime: 3 * time.Second, checker: checker})
	op4 := op3.AddNext("op4", &SimpleOp{data: "op4_data", processTime: 4 * time.Second, checker: checker})
	op2.AddNextNode(op4)
	nodeMap["op1"] = op1
	nodeMap["op2"] = op2
	nodeMap["op3"] = op3
	nodeMap["op4"] = op4

	var dag DAG
	r := dag.Init(start, nil)
	assert.True(t, r)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute(context.TODO())
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))

	sk := dag.GetStateKeeper()
	outputs := sk.GetAllOutput()
	assert.Equal(t, 4, len(outputs))
	for k, v := range outputs {
		assert.Equal(t, v, k + "_data")
	}
}

func TestTimeout(t *testing.T) {
	fmt.Println("TestTimeout...")
	start := NewStartNode("start")
	op1 := start.AddNext("op1", &SimpleOp{data: "op1_data", processTime: 1 * time.Second})
	op2 := start.AddNext("op2", &SimpleOp{data: "op2_data", processTime: 2 * time.Second})
	op3 := op1.AddNext("op3", &SimpleOp{data: "op3_data", processTime: 3 * time.Second}).WithTimeout(2 * time.Second)
	op4 := op3.AddNext("op4", &SimpleOp{data: "op4_data", processTime: 4 * time.Second})
	op2.AddNextNode(op4)

	var dag DAG
	r := dag.Init(start, nil)
	assert.True(t, r)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute(context.TODO())
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))

	sk := dag.GetStateKeeper()
	outputs := sk.GetAllOutput()
	fmt.Println(outputs)
	assert.Equal(t, nil, outputs["op3"])
	for k, v := range outputs {
		assert.Equal(t, v, k + "_data")
	}
}

