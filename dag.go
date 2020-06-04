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
	state map[string]interface{}
}

func (sk *DefaultStateKeeper) SetInput(id string, input interface{}) {
	sk.state[id] = input
}

func (sk *DefaultStateKeeper) GetInput(id string, fromID string) interface {} {
	return sk.state[id]
}

func (sk *DefaultStateKeeper) SetOutput(id string, output interface{}) {
	sk.state[id] = output
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
		p.stateKeeper = &DefaultStateKeeper{
			state: make(map[string]interface{}),
		}
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
			args := make([]interface{}, len(node.prev))
			for idx := range node.prev {
				args[idx] = p.stateKeeper.GetInput(node.prev[idx].id, node.id)
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
