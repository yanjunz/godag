package main

import (
	"context"
	"fmt"
	"time"
	"git.code.oa.com/video-fdmc/godag"
)

// 特征配置文件每一行会变成ConfItem
// 如：name=ds1;func=SOP_leftjoin(_ds1, _ds2)
type ConfItem struct {
	Name        string		// 名称
	Func        string
	FeaParams   []string	// 入参，相当于父节点
	ConstParams []string
}
// 用来给Conf描述Dag节点关系
type ConfDagNode struct {
	Name     string
	Item     *ConfItem
	Children []*ConfDagNode
}
// Conf 持有所有的特征描述配置条目，以及建立好的DAG起始节点
type Conf struct {
	ConfItems []ConfItem
	StartNode *ConfDagNode
	Nodes map[string]*ConfDagNode
}

const START_NODE = "__start__"
func (c *Conf) BuildDAG() bool {
	nodeMap := make(map[string]*ConfDagNode)
	curNode := &ConfDagNode{
		Name: START_NODE,
	}
	c.StartNode = curNode
	nodeMap[curNode.Name] = curNode	// add start
	parentNames := []string{START_NODE}
	nodeMap[START_NODE] = curNode
	for i := 0; i < len(c.ConfItems); i++ {
		if c.ConfItems[i].Func == "SOP_select" {
			parentNames = []string{START_NODE}
		} else {
			parentNames = c.ConfItems[i].FeaParams
		}
		newNode := &ConfDagNode{
			Name: c.ConfItems[i].Name,
			Item: &c.ConfItems[i],
		}
		for _, p := range parentNames {
			parentNode, ok := nodeMap[p]
			if !ok {
				fmt.Println("[ERROR] Unable to find parent Node", p, c.ConfItems[i])
				return false
			}
			parentNode.Children = append(parentNode.Children, newNode)
		}
		nodeMap[newNode.Name] = newNode
	}
	c.Nodes = nodeMap
	return true
}

/**
name=ds1;func=SOP_select(a)
name=ds2;func=SOP_select(b)
name=ds3;func=SOP_select(c)
name=ds_all;func=SOP_union(ds1, ds2)
name=ds_join;func=SOP_leftjoin(ds3, ds_all)
name=ds_group;func=SOP_groupby(ds2, ds3, ds_all)
**/
func prepareMockConf() *Conf {
	// 省去词法语法解释，直接上配置
	conf := Conf {
		ConfItems: []ConfItem {
			ConfItem{"ds1", "SOP_select", []string{}, []string{"a"}},
			ConfItem{"ds2", "SOP_select", []string{}, []string{"b"}},
			ConfItem{"ds3", "SOP_select", []string{}, []string{"c"}},
			ConfItem{"ds_all", "SOP_union", []string{"ds1", "ds2"}, []string{}},
			ConfItem{"ds_join", "SOP_leftjoin", []string{"ds3", "ds_all"}, []string{}},
			ConfItem{"ds_group", "SOP_groupby", []string{"ds2", "ds3", "ds_all"}, []string{}},
		},
	}
	conf.BuildDAG()
	return &conf
}

func parseConfDagNode(dagNode *ConfDagNode, confNodes map[string]*ConfDagNode, nodeMap *map[string]*godag.Node) (retNode *godag.Node, retCode int) {
	// 先深度遍历构造所有节点
	var leafNodes []*ConfDagNode
	retNode, retCode = deepWalkCreateNode(dagNode, nodeMap, &leafNodes)
	fmt.Println(leafNodes)
	// 从叶子节点开始反向构建DAG
	edgeMap := make(map[string]bool)	// edge for parent => child, key = parent:child
	for _, leafNode := range leafNodes {
		reverseBuildDAG(leafNode, confNodes, *nodeMap, &edgeMap)
	}
	return
}

func deepWalkCreateNode(dagNode *ConfDagNode, nodeMap *map[string]*godag.Node, leafNodes *[]*ConfDagNode) (retNode *godag.Node, retCode int) {
	if dagNode.Item == nil {
		retNode = godag.NewStartNode(dagNode.Name)
		(*nodeMap)[dagNode.Name] = retNode
	} else {
		if _, ok := (*nodeMap)[dagNode.Name]; !ok {
			op := createOpAdapter(dagNode)
			retNode = godag.NewNode(dagNode.Name, op)
			(*nodeMap)[dagNode.Name] = retNode
		}
	}

	if len(dagNode.Children) == 0 {
		*leafNodes = append(*leafNodes, dagNode)
	}

	for _, child := range dagNode.Children {
		_, retCode = deepWalkCreateNode(child, nodeMap, leafNodes)
		if retCode != 0 {
			return
		}
	}
	return
}

func reverseBuildDAG(dagNode *ConfDagNode, confNodes map[string]*ConfDagNode, nodeMap map[string]*godag.Node, edgeMap *map[string]bool) {
	curNode := nodeMap[dagNode.Name]
	if len(dagNode.Item.FeaParams) == 0 { // source operator
		curNode.AddPrevNode(nodeMap[START_NODE])
		return
	}

	for _, parentNodeName := range dagNode.Item.FeaParams {
		parentNode, ok := nodeMap[parentNodeName]
		if !ok {
			panic("node missed: " + parentNodeName)
		}
		edge := parentNodeName + ":" + dagNode.Name
		if _, ok := (*edgeMap)[edge]; !ok {
			curNode.AddPrevNode(parentNode)
			(*edgeMap)[edge] = true
			fmt.Println("Add edge ", edge)
			reverseBuildDAG(confNodes[parentNodeName], confNodes, nodeMap, edgeMap)
		}
	}
}

type SimpleOp struct {
	data        string
}

func (o *SimpleOp) Process(ctx context.Context, global interface{}, input ...interface{}) interface{} {
	fmt.Println(time.Now(), "Process", o.data, input)
	return o.data
}
func createOpAdapter(dagNode *ConfDagNode) godag.Op {
	return &SimpleOp{
		data: fmt.Sprintf(" %s(%v, %v) ", dagNode.Item.Func, dagNode.Item.FeaParams, dagNode.Item.ConstParams),
	}
}

func main() {
	conf := prepareMockConf()
	var dag godag.DAG
	nodeMap := make(map[string]*godag.Node)
	startNode, ret := parseConfDagNode(conf.StartNode, conf.Nodes, &nodeMap)
	fmt.Println("parseConfDagNode ", startNode, ret)
	dag.Init(startNode, nil)
	dag.Execute(context.TODO())
	fmt.Println("finish")
	fmt.Println(dag.GetStateKeeper().GetAllOutput())
}