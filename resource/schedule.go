package resource

import (
	"fmt"
	"sort"
)

type Scheduler interface {
	Schedule(resource *Resource,pls []plan) ([]SchePlan,error)
	BooK(resource *Resource,num int32) ([]ScheBook,error)
}
type Strategy interface {
	Choice(resource *Resource,num int32) ([]Node,error)
}
type SchePlan struct {
	nodeName string
	plan     plan
}
type ScheBook struct {
	nodeName string
	total int32
}
type StandardMode struct {
	strategy Strategy
}
func (s *StandardMode) Schedule(resource *Resource,pls []plan) ([]SchePlan,error){
	nodes, err := s.strategy.Choice(resource, int32(len(pls)))
	if err != nil {
		return nil,err
	}
	list := make([]SchePlan,0,len(nodes))
	index := 0
	for _, node := range nodes {
		for j := int32(0); j < node.total; j++ {
			list = append(list, SchePlan{
				nodeName: node.name,
				plan:   pls[index],
			})
			index++
		}
	}
	return list,nil
}

func (s *StandardMode) BooK(resource *Resource, num int32) ([]ScheBook, error) {
	nodes, err := s.strategy.Choice(resource,num)
	if err != nil {
		return nil,err
	}
	list := make([]ScheBook,0,len(nodes))
	for _, node := range nodes {
		list = append(list, ScheBook{
			nodeName: node.name,
			total:    node.total,
		})
	}
	return list,nil
}
func NewLeastNodeScheduler() *LeastNodeScheduler {
	r := &LeastNodeScheduler{}
	r.StandardMode = StandardMode{r}
	return r
}
/*最少链接*/
type LeastNodeScheduler struct {
	StandardMode
}
func (p *LeastNodeScheduler) Choice(resource *Resource,num int32) ([]Node,error)  {
	nodes := make([]Node,0,len(resource.nodes))
	total := int32(0)
	for _, nodeResource := range resource.nodes {
		if nodeResource.freeNum >0 {
			nodes = append(nodes, Node{
				name:     nodeResource.nodeName,
				priority: nodeResource.priority,
				total:    nodeResource.freeNum,
			})
			total += nodeResource.freeNum
		}
	}
	if total<num {
		return nil,fmt.Errorf("don't satisfied, need %d, left:%d",num,total)
	}
	if total == num {
		return nodes,nil
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].total >nodes[j].total
	})
	choiceNodes := make([]Node,0,num)
	index := 0
	nodeCt := len(nodes)
	for num>0{
		index = index%nodeCt

		nextIndex := index+1
		if index == nodeCt-1 {
			nextIndex = 0
		}
		if nodes[index].total<nodes[nextIndex].total{
			index++
			continue
		}
		choiceNodes = append(choiceNodes, Node{
			name:     nodes[index].name,
			priority: 1,
			total:    1,
		})
		nodes[index].total--
		num--
	}
	return choiceNodes,nil
}
func NewPollingNodeScheduler() *PollingNodeScheduler {
	r := &PollingNodeScheduler{
		index:        0,
	}

	r.StandardMode = &StandardMode{}
	r.strategy = r
	return r
}
/*轮询*/
type  PollingNodeScheduler struct {
	index int
	*StandardMode
}

func (s *PollingNodeScheduler)  Choice(resource *Resource,num int32) ([]Node,error)  {
	nodeCt := len(resource.nodes)
	nodeRes := make([]NodeResource,0,nodeCt)
	total := int32(0)
	for _, nr := range resource.nodes {
		nodeRes = append(nodeRes, NodeResource{
			priority: nr.priority,
			nodeName: nr.nodeName,
			total:    nr.total,
			freeNum:  nr.freeNum,
			bookNum:  nr.bookNum,
		})
		total += nr.freeNum
	}
	if total<num {
		return nil,fmt.Errorf("don't satisfied, need %d, left:%d",num,total)
	}
	sort.Slice(nodeRes, func(i, j int) bool {
		return nodeRes[i].nodeName<nodeRes[j].nodeName
	})
	nodes := make([]Node,0,num)
	for num>0 {
		index := s.index%nodeCt
		if nodeRes[index].freeNum>0 {
			nodes = append(nodes, Node{
				name:     nodeRes[index].nodeName,
				priority: nodeRes[index].priority,
				total:    1,
			})
			nodeRes[index].freeNum--
			num--
		}
		s.index++
	}
	return nodes,nil
}
func NewSameNodeScheduler() *SameNodeScheduler {
	r := &SameNodeScheduler{}
	r.StandardMode = StandardMode{r}
	return r
}

/*优先给同一个节点*/
type  SameNodeScheduler struct {
	StandardMode
}

func (s SameNodeScheduler)  Choice(resource *Resource,num int32) ([]Node,error)  {
	nodes := make([]Node,0,len(resource.nodes))
	total := int32(0)
	for _, nodeResource := range resource.nodes {
		if nodeResource.freeNum >0 {
			nodes = append(nodes, Node{
				name:     nodeResource.nodeName,
				priority: nodeResource.priority,
				total:    nodeResource.freeNum,
			})
			total += nodeResource.freeNum
		}
	}
	if total<num {
		return nil,fmt.Errorf("don't satisfied, need %d, left:%d",num,total)
	}
	if total == num {
		return nodes,nil
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].name<nodes[j].name && nodes[i].total<nodes[j].total
	})
	index := 0
	choiceNodes := make([]Node,0,num)
	for i := 0; i < len(nodes); i++ {
		if nodes[i].total<num{
			index = i
			continue
		}
		break
	}
	for i := index; i>=0 && num>0; i-- {
		resNum := num
		if nodes[i].total<resNum {
			resNum = nodes[i].total
		}
		choiceNodes = append(choiceNodes, Node{
			name:     nodes[i].name,
			priority: 1,
			total:    resNum,
		})
		nodes[i].total -= resNum
		num -= resNum
	}
	return choiceNodes,nil
}

func NewPriorityScheduler() *PriorityScheduler {
	r := &PriorityScheduler{}
	r.StandardMode = StandardMode{r}
	return r
}

/*根据优先级分配*/
type PriorityScheduler struct {
	StandardMode
}

func (p PriorityScheduler) Choice(resource *Resource,num int32) ([]Node,error)  {
	nodes := make([]Node,0,len(resource.nodes))
	total := int32(0)
	for _, nodeResource := range resource.nodes {
		if nodeResource.freeNum >0 {
			nodes = append(nodes, Node{
				name:     nodeResource.nodeName,
				priority: nodeResource.priority,
				total:    nodeResource.freeNum,
			})
			total += nodeResource.freeNum
		}
	}
	if total<num {
		return nil,fmt.Errorf("don't satisfied, need %d, left:%d",num,total)
	}
	if total == num {
		return nodes,nil
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].priority>nodes[j].priority
	})
	choiceNodes := make([]Node,0,num)
	index := 0
	for i := int32(0); i < num; i++ {
		for nodes[index].total<1 && index<len(nodes){
			index++
		}
		if nodes[index].total>0 {
			choiceNodes = append(choiceNodes, Node{
				name:     nodes[index].name,
				priority: 1,
				total:    1,
			})
			nodes[index].total--
		}
	}
	return choiceNodes,nil
}

type Node struct {
	name     string
	priority PLevel
	total    int32
}


var resourceScheduler Scheduler
func DeafultScheduler() Scheduler {
	if resourceScheduler == nil {
		resourceScheduler = &LeastNodeScheduler{}
	}
	return resourceScheduler
}

