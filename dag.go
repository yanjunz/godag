package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Op interface {
	Process(ctx context.Context, input interface{}) interface{}
}

type Node struct {
	name       string
	op         Op
	prev       []*Node
	next       []*Node
	timeout    time.Duration
	isCanceled bool
	mu         sync.Mutex
	indegree   int
	costTime   time.Duration
}

func (n *Node) AddNext(name string, op Op) *Node {
	newNode := Node{
		name:     name,
		op:       op,
		prev:     []*Node{n},
		indegree: 1,
	}
	n.next = append(n.next, &newNode)
	return &newNode
}

func (n *Node) AddNextNode(node *Node) *Node {
	n.next = append(n.next, node)
	node.prev = append(node.prev, n)
	node.indegree++
	return node
}

type DAG struct {
	startNode *Node
	// taskQueue *list.List
	// queueLock sync.Mutex
	mu        sync.Mutex
	activeNum int
	taskChan  chan *Node
	doneChan  chan struct{}
}

func (p *DAG) Init(startNode *Node) bool {
	p.startNode = startNode
	p.activeNum = 1

	// p.taskQueue = list.New()
	// p.taskQueue.PushBack(startNode)
	p.taskChan = make(chan *Node)
	p.doneChan = make(chan struct{})
	go func() {
		p.taskChan <- startNode
	}()
	return true
}

func (p *DAG) Execute() {
	for {
		select {
		case node := <-p.taskChan:
			go p.ProcessNode(node)
		case <-p.doneChan:
			return
		}
	}
}

func (p *DAG) ProcessNode(node *Node) {
	ctx := context.Background()
	if node.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, node.timeout)
		defer cancel()
	}

	doneChan := make(chan struct{})
	startTime := time.Now()
	go func() {
		if node.op != nil {
			node.op.Process(ctx, nil)
		}
		close(doneChan) // close can make chan readable
	}()
	select {
	case <-ctx.Done():
		node.isCanceled = true
	case <-doneChan:
		node.isCanceled = false
	}
	node.costTime = time.Now().Sub(startTime)
	go func() {
		for _, nextOne := range node.next {
			nextOne.mu.Lock()
			nextOne.indegree--
			indegree := nextOne.indegree
			nextOne.mu.Unlock()
			if indegree == 0 {
				p.mu.Lock()
				p.activeNum++ // should add before chan put
				p.mu.Unlock()
				p.taskChan <- nextOne
			}
		}
		p.mu.Lock()
		p.activeNum--
		activeNum := p.activeNum
		p.mu.Unlock()
		if activeNum == 0 {
			close(p.doneChan)
		}
	}()
}

type SimpleOp struct {
	data        string
	processTime time.Duration
}

func (o *SimpleOp) Process(ctx context.Context, input interface{}) interface{} {
	fmt.Println(time.Now(), "Process begin", o.data, o.processTime)
	time.Sleep(o.processTime)
	fmt.Println(time.Now(), "Process done ", o.data, o.processTime)
	return nil
}

func main() {
	start := Node{name: "start"}
	op1 := start.AddNext("op1", &SimpleOp{data: "op1_data", processTime: 1 * time.Second})
	op2 := start.AddNext("op2", &SimpleOp{data: "op2_data", processTime: 2 * time.Second})
	op3 := op1.AddNext("op3", &SimpleOp{data: "op3_data", processTime: 3 * time.Second})
	op4 := op3.AddNext("op4", &SimpleOp{data: "op4_data", processTime: 4 * time.Second})
	op2.AddNextNode(op4)

	var dag DAG
	dag.Init(&start)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute()
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))
}
