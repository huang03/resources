package resource

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type PLevel int

type PlanDeliveryFunc func(p plan,node string) error

func DefaultPlanDeliverFunc() PlanDeliveryFunc {
	return func(p plan, node string) error {
		fmt.Printf("plan:%v,nodeName:%s\n",p,node)
		return nil
	}
}
func NewResource() *Resource {
	r := &Resource{
		rmux:    sync.RWMutex{},
		bks:     make(map[string]*books),
		nodes:   make(map[string]*NodeResource),
		total:   0,
		freeNum: 0,
		bookNum: 0,
	}
	return r
}

type Resource struct {
	rmux  sync.RWMutex
	bks map[string]*books
	nodes map[string]*NodeResource
	total int32 //资源总数
	freeNum int32
	bookNum int32
}

func (r *Resource) String() string  {
	r.rmux.RLock()
	defer r.rmux.RUnlock()
	strs := make([]string,0)
	strs = append(strs,fmt.Sprintf("total:%d,freeNum:%d,bookNum:%d\n***********nodes***********\n",r.total,r.freeNum,r.bookNum))
	for _, n := range r.nodes {
		strs = append(strs,n.String())
	}
	strs = append(strs,"***********nodes end***********\n")
	strs = append(strs,"***********books***********\n")
	for _, bks := range r.bks {
		strs = append(strs,bks.String())
	}
	strs = append(strs,"***********books end***********\n")
	return strings.Join(strs,"")
}

func (r *Resource) Schedule(pls plans) ([]plan,error)  {
	if pls.scheduler == nil {
		pls.scheduler = DeafultScheduler()
	}
	if pls.delivery == nil {
		pls.delivery = DefaultPlanDeliverFunc()
	}
	r.rmux.Lock()
	defer r.rmux.Unlock()
	r.recycleBook()
	result,err := pls.scheduler.Schedule(r,pls.pls)
	if err != nil {
		return pls.pls,err
	}
	failPlans := make([]plan,0)
	var failErr error
	for _,splan :=range result {
		err = pls.delivery(splan.plan,splan.nodeName)
		if err != nil {
			if failErr == nil {
				failErr = err
			}
			fmt.Printf("schedule fail plan:%+v,nodeName:%s,err:%s \n",splan.plan,splan.nodeName,err.Error())
			failPlans = append(failPlans,splan.plan)
			continue
		}
		fmt.Printf("schedule success plan:%+v,nodeName:%s \n",splan.plan,splan.nodeName)
		r.decr(splan.nodeName,1)
	}
	return failPlans,failErr
}
func (r *Resource) Reset()  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	r.freeNum = 0
	r.bookNum = 0
	for _, node := range r.nodes {
		node.freeNum = node.total
		node.bookNum = 0
		r.freeNum += node.freeNum
	}

	r.bks = make(map[string]*books,0)
}
func (r *Resource) Recycle(nodeName string,delta int32)  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; !ok{
		return
	}
	r.incr(nodeName,delta)
}
func (r *Resource) recycleBook()  {
	for bizName, bks := range r.bks {
		if bks.Expire() {
			delete(r.bks,bizName)
		}
	}
}
func (r *Resource) DoBook(bizName string,pls plans) ([]plan,error) {
	if pls.delivery == nil {
		return pls.pls,fmt.Errorf("bizName:%s delivery is nil",bizName)
	}
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.bks[bizName];!ok {
		return pls.pls,fmt.Errorf("bizName:%s don't exist",bizName)
	}
	bks := r.bks[bizName]
	if bks.Expire() {
		return pls.pls,fmt.Errorf("bizName:%s has expireAt:%d",bizName,bks.expireAt.Unix())
	}
	total := 0
	nodeNames := make([]string,0,len(bks.res))
	for _, b := range bks.res {
		total += int(b.total)
		nodeNames = append(nodeNames,b.node)
	}
	if total<len(pls.pls){
		return pls.pls,fmt.Errorf("bizName:%s need:%d,but book total is %d",bizName,len(pls.pls),total)
	}
	failPlans := make([]plan,0)
	var failErr error
	index := 0
	for _, pl := range pls.pls {
		var bk *book
		for index<len(nodeNames){
			bk = bks.res[nodeNames[index]]
			if bk.total<1{
				index++
				continue
			}
			break
		}
		if bk == nil {
			failPlans = append(failPlans,pl)
			continue
		}
		err :=  pls.delivery(pl,bk.node)
		if  err != nil && failErr == nil {
			failErr = err
		}
		if err != nil {
			failPlans = append(failPlans,pl)
			continue
		}
		r.nodes[bk.node].bookNum--
		total--
		bk.total--
		r.bookNum--
	}
	if total == 0 {
		delete(r.bks,bizName)
	}
	return failPlans,failErr
}
func (r *Resource) Book(bizName string,timeOut time.Duration,num int32,scheduler Scheduler) error {
	if num<1 {
		return fmt.Errorf("book num < 1")
	}

	r.rmux.Lock()
	defer r.rmux.Unlock()
	r.recycleBook()
	if _,ok := r.bks[bizName];ok {
		return fmt.Errorf("bizName:%s exist",bizName)
	}
	nodes,err := scheduler.BooK(r,num)
	if err != nil {
		return err
	}

	bk := &books{
		bizName: bizName,
		timeout:    timeOut,
		expireAt: time.Now().Add(timeOut),
		res:        make(map[string]*book,0),
	}
	for _, bookInfo := range nodes {
		fmt.Printf("book nodeName:%s,total:%d success \n",bookInfo.nodeName,bookInfo.total)
		if _,ok :=bk.res[bookInfo.nodeName];ok {
			bk.res[bookInfo.nodeName].total += bookInfo.total
			r.incrBook(bookInfo.nodeName,bookInfo.total)
			continue
		}
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
	r.bookNum +=delta
	r.nodes[nodeName].incrBook(delta)
	r.nodes[nodeName].decr(delta)
}
func (r *Resource) decrBook(nodeName string,delta int32)  {
	r.freeNum +=delta
	r.bookNum -=delta
	r.nodes[nodeName].decrBook(delta)
	r.nodes[nodeName].incr(delta)
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
func (r *Resource) RemoveNode(nodeName string) bool  {
	r.rmux.Lock()
	defer r.rmux.Unlock()
	if _,ok := r.nodes[nodeName]; !ok {
		return true
	}
	nr := r.nodes[nodeName]
	//预定资源的处理？？？
	if nr.bookNum >0 {

	}
	r.total -= nr.total
	r.freeNum -= nr.freeNum
	r.bookNum -= nr.bookNum
	delete(r.nodes,nr.nodeName)
	return true
}
func  NewNodeResource(nodeName string,total int32) *NodeResource {
	return &NodeResource{
		priority: PLevel(50),
		nodeName: nodeName,
		total:    total,
		freeNum:  total,
		bookNum:  0,
	}
}
func (nr *NodeResource) String() string  {
	return fmt.Sprintf("nodeName:%s,priority:%d,total:%d,freeNum:%d,bookNum:%d \n",nr.nodeName,nr.priority,nr.total,nr.freeNum,nr.bookNum)
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
	total    int32 //总资源
	freeNum  int32
	bookNum  int32
}

type books struct {
	bizName string
	timeout time.Duration
	expireAt time.Time
	res map[string]*book
}

func (bks books) Expire() bool {
	if bks.expireAt.Unix()>=time.Now().Unix(){
		return false
	}
	return true
}
func (bks *books) String() string {
	strs := make([]string,0)
	strs = append(strs,fmt.Sprintf("books bizName:%s,expireAt:%d,timeout:%ds\n",bks.bizName,bks.expireAt.Unix(),bks.timeout/time.Second))
	for _, b := range bks.res {
		strs = append(strs,fmt.Sprintf("book nodeName:%s,total:%d\n",b.node,b.total))
	}
	return strings.Join(strs,"")
}
//预定资源
type book struct {
	node string
	total int32
}
type plans struct {
	scheduler Scheduler
	delivery  PlanDeliveryFunc
	bizName   string
	pls       []plan
}
func (p *plans) Append(key string,data interface{})  {
	p.pls = append(p.pls, plan{
		Key:  key,
		Data: data,
	})
}
func (p *plans) Scheduler(scheduler Scheduler)  {
	p.scheduler = scheduler
}
func NewPlans(bizName string,delivery PlanDeliveryFunc,scheduler Scheduler,pls ...plan) plans {
	return plans{
		bizName: bizName,
		delivery: delivery,
		scheduler: scheduler,
		pls: pls,
	}
}
type plan struct {
	Key string
	Data interface{}
}

func NewPlan(key string,data interface{}) plan {
	return plan{key,data}
}

