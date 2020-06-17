package godag

import (
	"context"
	"sync"
	"time"
	// "fmt"
)

type Op interface {
	// global: global variable to pass through DAG
	// input: the output generated by parent (the order is the same as node.prev)
	Process(ctx context.Context, global interface{}, input ...interface{}) interface{} // pass input with the order of prev
}

// StateKeeper  用来在DAG中传递数据，
// 1. Parent的输出是child的输入
// 2. Parent的输出对于不同的child的输入可能是不一样的
type StateKeeper interface {
	SetInput(curID string, input interface{})           // set the input of "curID"
	GetInput(parentID string, curID string) interface{} // get the input of "curID" generate by "parentID"
	SetOutput(curID string, output interface{})         // set the output of "curID"
	GetOutput(curID string) interface{}                 // get the output of "curID"
	GetAllOutput() map[string]interface{}               // 获取所有的输出
	SetGlobal(global interface{})                       // set global state
	GetGlobal() interface{}                             // get global state
	ClearAll()                                          // clear all
}

type DefaultStateKeeper struct {
	mu     sync.Mutex
	State  map[string]interface{}
	Global interface{}
}

func NewDefaultStateKeeper() *DefaultStateKeeper {
	return &DefaultStateKeeper{
		State: make(map[string]interface{}),
	}
}

func (sk *DefaultStateKeeper) SetInput(curID string, input interface{}) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.State[curID] = input
}

func (sk *DefaultStateKeeper) GetInput(parentID string, curID string) interface{} {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	return sk.State[parentID]
}

func (sk *DefaultStateKeeper) SetOutput(curID string, output interface{}) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.State[curID] = output
}

func (sk *DefaultStateKeeper) GetOutput(curID string) interface{} {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	return sk.State[curID]
}

func (sk *DefaultStateKeeper) GetAllOutput() map[string]interface{} {
	return sk.State
}

func (sk *DefaultStateKeeper) GetGlobal() interface{} {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	return sk.Global
}

func (sk *DefaultStateKeeper) SetGlobal(global interface{}) {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.Global = global
}

func (sk *DefaultStateKeeper) ClearAll() {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	sk.State = make(map[string]interface{})
}

type StateKey string

const NodeID = "__nodeID__"

type Node struct {
	id         string // id should be unique
	op         Op
	prev       []*Node
	next       []*Node
	timeout    time.Duration
	isCanceled bool
	indegree   int
	costTime   time.Duration
}

func NewStartNode(id string) *Node {
	return &Node{
		id: id,
	}
}

func (n *Node) AddNext(id string, op Op) *Node {
	newNode := Node{
		id:       id,
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
	startNode   *Node
	mu          sync.Mutex
	activeNum   int
	taskChan    chan *Node
	doneChan    chan struct{}
	stateKeeper StateKeeper
}

func (p *DAG) Init(startNode *Node, stateKeeper StateKeeper) bool {
	p.startNode = startNode
	if stateKeeper == nil {
		p.stateKeeper = NewDefaultStateKeeper()
	} else {
		p.stateKeeper = stateKeeper
	}
	p.activeNum = 1
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
			go p.processNode(node)
		case <-p.doneChan:
			return
		}
	}
}

func (p *DAG) processNode(node *Node) {
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
			args := make([]interface{}, len(node.prev))
			for idx := range node.prev {
				args[idx] = p.stateKeeper.GetInput(node.prev[idx].id, node.id) // will get the parent output as input of current
			}
			ctx = context.WithValue(ctx, StateKey(NodeID), node.id)
			global := p.stateKeeper.GetGlobal()
			output := node.op.Process(ctx, global, args...)
			p.stateKeeper.SetOutput(node.id, output)
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
			p.mu.Lock()
			nextOne.indegree--
			indegree := nextOne.indegree
			p.mu.Unlock()
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

func (d *DAG) GetStateKeeper() StateKeeper {
	return d.stateKeeper
}
