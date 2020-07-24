package godag

import (
	"sync"
)

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