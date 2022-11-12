package temp

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
/*
1.资源管理， node，node处理资源的能力，node 处理能力
2.PUSH,GET, Reset,资源矫正
3.分配节点策略，优先分配同一个node,还是优先分配到不懂节点，或者根据节点能力来优先分配
4.如果资源不足时的分配策略，
5.预定资源、预定超时
6.任务优先级。
*/
/*
资源：
1.假设资源为 node 节点，每个节点可以处理任务的数量。 比如每个节点同时处理3个任务， res = node数量*3
2.资源分为两种，抢占资源和预定资源。
3.大任务有多个小任务组成
4.一个小任务需要一个资源处理。

资源计算方式：
1.变量包括  资源总数(total)、小任务总数(planNum)、用户预期时间(expectTime)、小任务最快完成时间(consumeTime)
2.大任务最快处理完时间为：minTime = (planNum/total)*consumeTime
3.大任务最晚完成时间为：maxTime := planNum*consumeTime
4.如果 expectTime>=maxTime 需要顺序执行，只需要绑定到一个节点的一个资源上。并根据预期时间，计算并发数，按并发数最大执行
5.如果 expectTime<minTime  这种情况，不存在，在用户设置的时候，就应该禁止。
6.如果 expectTime = minTime 按最大资源执行，有资源就执行
7.如果 minTime<expectTime<maxTime 计算需要多少资源同时并行才能执行。 resNum = maxTime/expectTime
   resNum 四舍五入(这里多少会有时间误差的，可能需要一定的补偿系数) 按并发数最大执行

调度策略：
1.情况4,7的情况预定资源，将所有小任务绑定在一个节点上。这个执行，必须有足够的资源，否则不可绑定
2.情况6 抢占空闲资源，
3.空闲资源包括 可用的抢占资源+可用的预定的资源。 这两个资源，在抢占资源不足时，会窃取空闲的预定资源。当使用完成之后归还

预期时间计算：
根据资源计算方式：
假设首个任务开始时间为startTime，资源分配与归还损耗为 x
4情况，理论完成时间[startTime+expectTime,startTime+expectTime+x]
5情况，不存在，无需讨论
6情况，理论完成时间[startTime+expectTime,startTime+expectTime+x]
7情况，理论完成时间[startTime+expectTime,startTime+expectTime+x]

这里有调度的时间损耗，其流程：
	Controller 分配执行计划 Node
    Node 执行计划
	Node 通过GRPC 归还资源

这个损耗x 需要监控获得一个比较好的系数。
*/

type Resource struct {
	rmux  sync.RWMutex
	bks map[int64]*books
	nodes map[string]*NodeResource
	total int32 //资源总数
	bookAvailable int32 //用于预定的可用资源
	freeAvailable int32 //自由使用的资源
	callStatis *statis
	maxConcurrent int
}
type statis struct {
	Total int32
	Suc int32
	Fail int32
	FromBook int32
	BookSuc int32
	BookFail int32
}
func (r *Resource) addStaticBookSuc()  {
	atomic.AddInt32(&r.callStatis.BookSuc,1)
}
func (r *Resource) addStaticBookFail()  {
	atomic.AddInt32(&r.callStatis.BookFail,1)
}
func (r *Resource) addStaticSuc(rtype int8)  {
	atomic.AddInt32(&r.callStatis.Suc,1)
	atomic.AddInt32(&r.callStatis.Total,1)
	if rtype == PLAN_TYPE_RACE_FROM_BOOK {
		atomic.AddInt32(&r.callStatis.FromBook,1)
	}
}
func (r *Resource) addStaticFail()  {
	atomic.AddInt32(&r.callStatis.Fail,1)
	atomic.AddInt32(&r.callStatis.Total,1)
}
func (r *Resource) String() string {
	r.rmux.RLock()
	r.rmux.RUnlock()
	str := fmt.Sprintf("Resource skip, nodeNum:%d,total:%d,bookAvailable:%d,freeAvailable:%d,bksNum:%d \r\n",
		len(r.nodes),r.total,r.bookAvailable,r.freeAvailable,len(r.bks))

	nodesInfos := make([]string,0,len(r.nodes))
	for _, nr := range r.nodes {
		tmp := "      "+nr.String()
		nodesInfos = append(nodesInfos,tmp)
	}
	booksInfo := make([]string,0,len(r.bks))
	for _, bs := range r.bks {
		tmp := "      "+bs.String()
		booksInfo = append(booksInfo,tmp)
	}
	str = fmt.Sprintf("%s    nodes info:\r\n%s\r\n    books info:\r\n%s",str,strings.Join(nodesInfos,"\r\n"),strings.Join(booksInfo,"\r\n"))
	str = fmt.Sprintf("%s\r\n static:%+v",str,r.callStatis)
	return str
}
type PlanConfig struct {
	id int64
	consumeTime int //单个任务的耗时
	expectTime  int //预期时间
	planId []int64
}

func NewPlanConfig(id int64,singleConsumeTime,expectTime int,planId []int64) PlanConfig {
	return PlanConfig{
		id:          id,
		consumeTime: singleConsumeTime,
		expectTime:  expectTime,
		planId:      planId,
	}
}

const (
	PLAN_TYPE_RACE = int8(1)
	PLAN_TYPE_RACE_FROM_BOOK = int8(2)
	PLAN_TYPE_BOOK = int8(3)
)

type Plan struct {
	Id int64
	NodeName string
	PlanId int64
	Type int8
}

func NewResource(maxConcurrent int) *Resource {
	res := &Resource{
		rmux:          sync.RWMutex{},
		bks:           make(map[int64]*books,0),
		nodes:         make(map[string]*NodeResource),
		total:         0,
		bookAvailable: 0,
		freeAvailable: 0,
		callStatis: &statis{},
		maxConcurrent: maxConcurrent,
	}
	return res
}

func (r *Resource) IsAvailable() bool {
	r.rmux.RLock()
	defer r.rmux.RUnlock()
	return r.bookAvailable>0 || r.freeAvailable>0
}
//获取资源
func (r *Resource) Get(id int64) (plans []Plan,find int)  {
	return r.searchResource(id)
}
//归还资源
func (r *Resource) Revert(id int64,nodeName string,rtype int8) bool  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; !ok {
		return false
	}
	if bks,ok := r.bks[id]; ok && rtype == PLAN_TYPE_BOOK {
		if bks.res[nodeName].available<bks.res[nodeName].resource{
			bks.res[nodeName].available++
		}
		fmt.Println("id----1-----",id)
		return true
	}
	if rtype == PLAN_TYPE_RACE_FROM_BOOK {
		if r.nodes[nodeName].bookAvailable<r.nodes[nodeName].bookNum+1 {
			r.nodes[nodeName].freeNum--
			r.nodes[nodeName].bookNum++
			r.nodes[nodeName].bookAvailable++
			r.bookAvailable++
		}

		return true
	}
	if rtype == PLAN_TYPE_RACE && r.nodes[nodeName].freeAvailable<r.nodes[nodeName].freeNum {
		r.nodes[nodeName].freeAvailable++
		r.freeAvailable++
		return  true
	}
	return true
}
func (r *Resource) CalResouce(config PlanConfig) (int,int) {
	return r.calResource(config)
}
//预定资源
func (r *Resource) TryBook(config PlanConfig) (bool,error)  {
	if len(config.planId) <1{
		return true,nil
	}
	id := config.id
	res,_ := r.calResource(config)
	resNum := int32(res)
	if resNum == -1 {
		return false,fmt.Errorf("invalid config:%v",config)
	}
	if resNum == 0 {
		return false,nil
	}

	r.rmux.Lock()
	defer r.rmux.Unlock()
	if r.freeAvailable<1 && r.bookAvailable<1  {
		r.addStaticBookFail()
		return false,fmt.Errorf("not enough resources,need(%d),has(0)",resNum)
	}
	if _,ok := r.bks[id]; ok {
		return false,nil
	}
	if r.bookAvailable<resNum {
		r.addStaticBookFail()
		return false,fmt.Errorf("not enough resources,need(%d),has(%d)",resNum,r.bookAvailable)
	}

	nodes := make([]NodeResource,0,len(r.nodes))
	for _, res := range r.nodes {
		nodes = append(nodes,*res)
	}
	//资源从大到小排序
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].bookAvailable>nodes[j].bookAvailable
	})
	r.bks[id] = &books{
		res:          make(map[string]*book),
		planId: make([]int64,len(config.planId)),
	}
	copy(r.bks[id].planId,config.planId)

	for i := 0; i < len(nodes); i++ {
		if resNum == 0 {
			break
		}
		if nodes[i].bookAvailable == 0 {
			break
		}
		if nodes[i].bookAvailable>=resNum { //一个节点能搞定，就不要搞两个节点了了
			nodes[i].bookAvailable -= resNum
			r.bks[id].res[nodes[i].nodeName] = &book{
				node:      nodes[i].nodeName,
				resource:  resNum,
				available: resNum,
			}
			r.bookAvailable -= resNum
			r.nodes[nodes[i].nodeName].bookAvailable -= resNum
			resNum = 0
			break
		} else { //一个节点搞不定，就来多来几个
			r.bks[id].res[nodes[i].nodeName] = &book{
				node:      nodes[i].nodeName,
				resource:  nodes[i].bookAvailable,
				available: nodes[i].bookAvailable,
			}
			r.bookAvailable -= nodes[i].bookAvailable
			r.nodes[nodes[i].nodeName].bookAvailable -= nodes[i].bookAvailable
			resNum -= nodes[i].bookAvailable
			nodes[i].bookAvailable = 0
		}
	}
	if resNum != 0 {
		r.addStaticBookFail()
		return false,fmt.Errorf("not enough resources,need(%d),has(%d)",resNum,r.bookAvailable)
	}
	r.addStaticBookSuc()
	return  true,nil
}
//取消预定
func (r *Resource) CancelBook(id int64)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.bks[id]; !ok {
		return
	}
	bks:= r.bks[id]
	for nodeName, bk := range bks.res {
		r.bookAvailable += bk.available
		r.nodes[nodeName].bookAvailable+=bk.available
	}
	delete(r.bks,id)
}
func (r *Resource) GetNode() []NodeResource  {
	r.rmux.RLock()
	defer r.rmux.RUnlock()
	data := make([]NodeResource,0,len(r.nodes))
	for _, node := range r.nodes {
		nr := NodeResource{
			nodeName:      node.nodeName,
			resource:      node.resource,
			freeNum:       node.freeNum,
			bookNum:       node.bookNum,
			freeAvailable: node.freeAvailable,
			bookAvailable: node.bookAvailable,
			availableTime: node.availableTime,
		}
		data = append(data,nr)
	}
	return data
}
//新增可用节点
func (r *Resource) AddNode(nodeName string,freeRes,bookRes int32)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; ok {
		return
	}
	r.nodes[nodeName] = &NodeResource{
		nodeName: nodeName,
		resource:      freeRes+bookRes,
		freeNum: freeRes,
		bookNum: bookRes,
		freeAvailable:     freeRes,
		bookAvailable: bookRes,
		availableTime: time.Now().Unix(),
	}
	r.total += freeRes+bookRes
	r.freeAvailable += freeRes
	r.bookAvailable += bookRes
}
//移除节点
func (r *Resource) RemoveNode(nodeName string)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; !ok {
		return
	}
	node := r.nodes[nodeName]
	r.total -= node.resource
	r.freeAvailable -= node.freeAvailable
	r.bookAvailable -= node.bookAvailable
	delete(r.nodes,nodeName)
}
/*
find:
 -1 预定，任务都完成了
 0 找不到资源
 1 找到了资源
*/
func (r *Resource) searchResource(id int64)(plans []Plan,find int)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	find = 1
	plans = make([]Plan,0)
	//抢占式
	if _,ok := r.bks[id]; !ok {
		if r.freeAvailable<1 && r.bookAvailable<1 {
			find = 0
			r.addStaticFail()
			return
		}
		if r.freeAvailable>0 {
			for nodeName,node  := range r.nodes {
				if node.freeAvailable>0 {
					node.freeAvailable--
					r.freeAvailable--
					//fmt.Println(node)
					plans = append(plans,Plan{
						NodeName: nodeName,
						PlanId:   0,
						Type: PLAN_TYPE_RACE,
						Id: id,
					})
					r.addStaticSuc(PLAN_TYPE_RACE)
					return
				}
			}
		}

		if r.bookAvailable>0 {
			for nodeName,node  := range r.nodes {
				if node.bookAvailable>0 {
					node.bookAvailable--
					r.bookAvailable--
					node.freeNum++
					node.bookNum--
					plans = append(plans,Plan{
						NodeName: nodeName,
						PlanId:   0,
						Type: PLAN_TYPE_RACE_FROM_BOOK,
						Id: id,
					})
					r.addStaticSuc(PLAN_TYPE_RACE_FROM_BOOK)
					return
				}
			}
		}
		find = 0
		return
	}

	bks := r.bks[id]
	//任务已经完成了
	if len(bks.planId)<1{
		find = -1
		return
	}
	//预定资源，将空闲资源都用上
	for nodeName, b := range bks.res {
		used := int32(0)
		for i := int32(0); i < b.available; i++ {
			plandId := bks.planId[0]
			bks.planId = bks.planId[1:]
			plans = append(plans,Plan{
				NodeName: nodeName,
				PlanId:   plandId,
				Type: PLAN_TYPE_BOOK,
				Id: id,
			})
			used++

			r.addStaticSuc(PLAN_TYPE_BOOK)
		}
		bks.res[nodeName].available -= used

	}
	return
}
// 0 表示不需要绑定任何资源，使用自由资源，尽可能发送就好了
/*
参数1
 -1 不可能完成
 0  抢占即可
 >0  需要预定资源个数

参数2
  并发数
*/
const (
	BOOKFAIL = -1
	BOOKNOTHING = 0
)

func (r *Resource) calResource(pcfg PlanConfig) (int,int) {
	min:=  (float64(len(pcfg.planId)) / float64(r.total))*float64(pcfg.consumeTime) //任务最快完成时间
	minTime := int(min)
	totalTime := len(pcfg.planId) * pcfg.consumeTime //顺序执行的总时间
	fmt.Printf("minTime:%d,maxTime:%d,expectTime:%d,signalTime:%d \r\n",minTime,totalTime,pcfg.expectTime,pcfg.consumeTime)
	if minTime>pcfg.expectTime {
		return BOOKFAIL,0
	}
	if minTime<pcfg.consumeTime {
		minTime = pcfg.consumeTime
	}
	if minTime >= pcfg.expectTime {
		return BOOKNOTHING,r.maxConcurrent
	}
	if pcfg.expectTime>totalTime { //顺序执行的总时间小于预期时间，那说明一个节点顺序执行就好了。并且要控制并发速度
		return 1,r.calConcurrent(pcfg.expectTime,totalTime)// 计算一下并发数
	}
	//6进
	res,_ := strconv.ParseFloat(fmt.Sprintf("%.0f", float64(totalTime)/float64(pcfg.expectTime)), 64)
	if res <1 {
		res = 1
	}
	resNum := int(res)
	return resNum,r.maxConcurrent
}
func (r *Resource) calConcurrent(expectTime int,totalTime int) (int) {
	if totalTime>expectTime {
		return r.maxConcurrent
	}
	rate := float64(totalTime)/float64(expectTime)
	curnent := rate*float64(r.maxConcurrent)
	return int(curnent)
}
type NodeResource struct {
	nodeName string
	resource int32 //总资源
	freeNum int32
	bookNum int32
	freeAvailable int32 //可用资源
	bookAvailable int32
	availableTime int64 //资源最早可用时间
}

func (nr *NodeResource) String() string  {
	return fmt.Sprintf("nodeName:%s,resource:%d,freeNum:%d,bookNum:%d,freeAvailable:%d,bookAvailable:%d",
		nr.nodeName,nr.resource,nr.freeNum,nr.bookNum,nr.freeAvailable,nr.bookAvailable)
}
//func (r *Resource) book() bool {
//	return atomic.LoadInt32(&r.bookAvailable)>0
//}
type books struct {
	res map[string]*book
	planId []int64
}

func (bs *books) String() string {
	infos := make([]string,0,len(bs.res))
	for _,b  := range bs.res {
		infos = append(infos,b.String())
	}
	return fmt.Sprintf("bookNodeNum:%d,planId:%v,info:\r\n%s ",len(bs.res),bs.planId,strings.Join(infos,"\r\n"))
}
//预定资源
type book struct {
	node string
	resource int32 //预定资源数量
	available int32 //剩余可使用
}

func (b *book) String() string  {
	return fmt.Sprintf("node:%s,resource:%d,available:%d",b.node,b.resource,b.available)
}

