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

/**
测试例子: 算子系统
name=ds1;func=SOP_select(mock_aikan_play);type=ds_select
name=ds2;func=SOP_select(mock_aikan_insert);type=ds_select
name=ds3;func=SOP_select(mock_aikan_exposure);type=ds_select

# 对exposure进行唯一性过滤，并作为原始序列输出，输出的key=ikan_exposure
name=ds_uniq_exp;func=SOP_unique(_ds3);ds_key=ikan_exposure;type=ds_transform

# 对insert和play2路进行合并
name=ds_all_play;func=SOP_union(_ds1, _ds2);type=ds_transform

# 对合并后的数据进行duration过滤（播放时长>5），并把这作为输出key=ikan_all_play
name=ds_valid_dur;func=SOP_duration_greater(_ds_all_play, 5);ds_key=ikan_all_play;type=ds_transform

####### 方式1：采用fe计算统计特征（按需定义虚拟属性、数据集抽取、特征抽取）
# 定义虚拟属性播放完成度play_rate=time_info.duration/video_info.duration
name=ds_valid_dur2;func=PROP_div(_ds_valid_dur, "play_rate", "time_info.duration", "session_seq.video_info.duration", "0");type=ds_prop

# 将数据集按特征格式进行抽取，合并成字符串feature
name=fe_session;func=SOP_extract(_ds_valid_dur2, "|", ":", "session_seq.expo_vids:time_info.play_time:play_rate");type=ds_conv

# 将字符串feature执行fe的parse_pvreal，得到统计特征，输出slot=0
name=fe_pvreal;func=OP_parse_pvreal(_fe_session, 0.5, 2);index=0;type=origin

######## 方式2：采用golang实现的数据集抽取，直接产出最终特征
# 调用golang的parse_pvreal计算得到统计特征，输出slot=1
name=fe_pvreal2;func=SOP_parse_pvreal(_ds_valid_dur, 0.5, 2);index=1;type=ds_conv
**/
func TestComplex(t *testing.T) {
	fmt.Println("TestComplex...")
	start := NewStartNode("start")

	ds1 := start.AddNext("ds1", &SimpleOp{data: "mock_aikan_play", processTime: 1 * time.Second})
	ds2 := start.AddNext("ds2", &SimpleOp{data: "mock_aikan_insert", processTime: 1 * time.Second})
	ds3 := start.AddNext("ds3", &SimpleOp{data: "mock_aikan_exposure", processTime: 1 * time.Second})
	ds_uniq_exp := ds3.AddNext("ds_uniq_exp", &SimpleOp{data: "ds_uniq_exp", processTime: 1 * time.Second})
	ds_all_play := ds1.AddNext("ds_all_play", &SimpleOp{data: "ds_all_play", processTime: 2 * time.Second})
	ds2.AddNextNode(ds_all_play)
	ds_valid_dur := ds_all_play.AddNext("ds_valid_dur", &SimpleOp{data: "ds_valid_dur", processTime: 3 * time.Second})
	ds_valid_dur2 := ds_valid_dur.AddNext("ds_valid_dur2", &SimpleOp{data: "ds_valid_dur2", processTime: 1 * time.Second})
	fe_session := ds_valid_dur2.AddNext("fe_session", &SimpleOp{data: "fe_session", processTime: 2 * time.Second})
	fe_pvreal := fe_session.AddNext("fe_pvreal", &SimpleOp{data: "fe_pvreal", processTime: 3 * time.Second})
	fe_pvreal2 := ds_valid_dur.AddNext("fe_pvreal2", &SimpleOp{data: "fe_pvreal2", processTime: 3 * time.Second})

	fmt.Println("output 3 op=", ds_uniq_exp, fe_pvreal, fe_pvreal2)
	var dag DAG
	r := dag.Init(start, nil)
	assert.True(t, r)
	startTime := time.Now()
	fmt.Println(startTime, "start")
	dag.Execute(context.TODO())
	endTime := time.Now()
	fmt.Println(endTime, "done", endTime.Sub(startTime))

	assert.Equal(t, len(dag.GetStateKeeper().GetAllOutput()), 10)
}

func TestLeftJoin(t *testing.T) {
	fmt.Println("TestComplex...")
	start := NewStartNode("start")

	ds1 := start.AddNext("ds1", &SimpleOp{data: "mock_aikan_play"})
	ds2 := start.AddNext("ds2", &SimpleOp{data: "mock_aikan_insert"})
	ds3 := start.AddNext("ds3", &SimpleOp{data: "mock_aikan_exposure"})
	ds4 := start.AddNext("ds4", &SimpleOp{data: "mock_aikan_exposure2"})
	ds_all_play := ds1.AddNext("ds_all_play", &SimpleOp{data: "ds_all_play"})
	ds2.AddNextNode(ds_all_play)
	// ds_left_join = SOP_left_join(ds4, ds_all_play, ds3)
	// 由于在构造DAG的时候如果采用深度遍历可能会导致有顺序依赖的输入会按AddNext无法保证顺序，需要用InsertPrevNode的方式进行调整
	ds_left_join := ds_all_play.AddNext("ds_left_join", &SimpleOp{data: "ds_left_join"})
	ds3.AddNextNode(ds_left_join)
	ds_left_join.InsertPrevNode(ds4, ds_all_play)

	assert.Equal(t, ds1.indegree, 1)
	assert.Equal(t, ds2.indegree, 1)
	assert.Equal(t, ds3.indegree, 1)
	assert.Equal(t, ds4.indegree, 1)
	assert.Equal(t, ds_all_play.indegree, 2)
	assert.Equal(t, ds_left_join.indegree, 3)
	assert.Equal(t, ds_left_join.prev[0], ds4)
	assert.Equal(t, ds_left_join.prev[1], ds_all_play)
	assert.Equal(t, ds_left_join.prev[2], ds3)
}