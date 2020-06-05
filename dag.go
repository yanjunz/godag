package godag

import (
	"context"
	"sync"
	"time"
	// "fmt"
)

type Op interface {
	Process(ctx context.Context, input ...interface{}) interface{}	// pass input with the order of prev
}

type StateKeeper interface {
	SetInput(id string, input interface{})				// set the input of "id" 
	GetInput(id string, fromID string) interface{} 		// get the input of "id" from "fromID"
	SetOutput(id string, output interface{})			// set the output of "id"
}

type DefaultStateKeeper struct {
	State map[string]interface{}
}

func NewDefaultStateKeeper() *DefaultStateKeeper {
	return &DefaultStateKeeper{
		State: make(map[string]interface{}),
	}
}

func (sk *DefaultStateKeeper) SetInput(curID string, input interface{}) {
	sk.State[curID] = input
}

func (sk *DefaultStateKeeper) GetInput(parentID string, curID string) interface {} {
	return sk.State[parentID]
}

func (sk *DefaultStateKeeper) SetOutput(curID string, output interface{}) {
	sk.State[curID] = output
}

type StateKey string

type Node struct {
	id         string	// id should be unique
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
		id:     id,
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
	mu        sync.Mutex
	activeNum int
	taskChan  chan *Node
	doneChan  chan struct{}
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
			ctx = context.WithValue(ctx, StateKey("id"), node.id)
			output := node.op.Process(ctx, args...)
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