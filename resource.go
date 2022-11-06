package resources

import (
	"fmt"
	"sync"
	"time"
)

/*
1.资源管理， node，node处理资源的能力，node 处理能力
2.PUSH,GET, Reset,资源矫正
3.分配节点策略，优先分配同一个node,还是优先分配到不懂节点，或者根据节点能力来优先分配
4.如果资源不足时的分配策略，
5.预定资源、预定超时
6.任务优先级。
7.任务投递


分配算法
预定

*/
type PLevel int

type PlanDeliveryFunc func(p plan,node string)


type Resource struct {
	rmux  sync.RWMutex
	bks map[string]*books
	nodes map[string]*NodeResource
	total int32 //资源总数
	freeNum int32
	bookNum int32
}

func (r *Resource) Plan(key string,data interface{}) plan {
	return plan{key,data}
}
func (r *Resource) CreatePlans(bizName string,delivery PlanDeliveryFunc) *plans  {
	return &plans{
		bizName: bizName,
		pls:     make([]plan,0),
		delivery: delivery,
	}
}
func (r *Resource) Schedule(pls *plans) error  {
	if pls.scheduler != nil {
		pls.scheduler = DeafultScheduler()
	}
	r.rmux.Lock()
	defer r.rmux.Unlock()
	result,err := pls.scheduler.Schedule(r,pls)
	if err != nil {
		return err
	}
	for _,splan :=range result {
		r.decr(splan.nodeName,1)
		pls.delivery(splan.plan,splan.nodeName)
	}
	return nil
}
func (r *Resource) Recycle(nodeName string,delta int32)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; !ok{
		return
	}
	r.incr(nodeName,delta)
}
func (r *Resource) Book(bizName string,timeOut time.Duration,num int32) error {
	if num<1 {
		return fmt.Errorf("book num < 1")
	}

	sche := DeafultScheduler()
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.bks[bizName];ok {
		return fmt.Errorf("bizName:%s exist",bizName)
	}
	nodes,err := sche.BooK(r,num)
	if err != nil {
		return err
	}

	bk := &books{
		timeout:    timeOut,
		createTime: time.Now(),
		res:        make(map[string]*book,0),
	}
	for _, bookInfo := range nodes {
		bk.res[bookInfo.nodeName] = &book{
			node:  bookInfo.nodeName,
			total: bookInfo.total,
		}
		r.incrBook(bookInfo.nodeName,bookInfo.total)
	}
	r.bks[bizName] = bk
	return nil
}
func (r *Resource) incrBook(nodeName string,delta int32)  {
	r.freeNum -=delta
	r.nodes[nodeName].incrBook(delta)
	//r.nodes[nodeName].decr(delta)
}
func (r *Resource) decrBook(nodeName string,delta int32)  {
	r.freeNum +=delta
	r.nodes[nodeName].decrBook(delta)
	//r.nodes[nodeName].incr(delta)
}
func (r *Resource) incr(nodeName string,delta int32)  {
	r.freeNum +=delta
	r.nodes[nodeName].incr(delta)
}
func (r *Resource) decr(nodeName string,delta int32)  {
	r.freeNum -=delta
	r.nodes[nodeName].decr(delta)
}
func (r *Resource) AddNode(nr *NodeResource) bool  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nr.nodeName]; ok {
		return false
	}
	r.nodes[nr.nodeName] = nr
	r.total += nr.total
	r.freeNum += nr.freeNum
	return  true
}
func (r Resource) RemoveNode(nr NodeResource) bool  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nr.nodeName]; !ok {
		return true
	}
	//预定资源的处理？？？
	if nr.bookNum >0 {

	}
	r.total -= nr.total
	r.freeNum -= nr.freeNum
	r.bookNum -= nr.bookNum
	delete(r.nodes,nr.nodeName)
	return true
}
func (r *Resource) NewNodeResource(nodeName string,total int32) *NodeResource {
	return &NodeResource{
		priority: PLevel(50),
		nodeName: nodeName,
		total:    total,
		freeNum:  total,
		bookNum:  0,
	}
}
func (nr *NodeResource) incr(delta int32)  {
	nr.freeNum += delta
}
func (nr *NodeResource) decr(delta int32)  {
	nr.freeNum -= delta
}
func (nr *NodeResource) incrBook(delta int32)  {
	nr.bookNum += delta
}
func (nr *NodeResource) decrBook(delta int32)  {
	nr.bookNum -= delta
}
func (nr *NodeResource) Priority(pl PLevel)  {
	if pl>100 {
		pl = PLevel(100)
	} else if pl<0 {
		pl = PLevel(0)
	}
	nr.priority = pl
}
type NodeResource struct {
	priority PLevel
	nodeName string
	total int32 //总资源
	freeNum int32
	bookNum int32
}

type books struct {
	timeout time.Duration
	createTime time.Time
	res map[string]*book
}
//预定资源
type book struct {
	node string
	total int32
}
type plans struct {
	scheduler Scheduler
	delivery PlanDeliveryFunc
	bizName string
	pls []plan
}
//func newPlans(plan ...plan) *plans {
//	return &plans{pls: plan}
//}
type plan struct {
	Key string
	Data interface{}
}

func (p *plans) Append(key string,data interface{})  {
	p.pls = append(p.pls,plan{
		Key:  key,
		Data: data,
	})
}
func (p *plans) Scheduler(scheduler Scheduler)  {
	p.scheduler = scheduler
}
