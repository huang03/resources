package resources

import (
	"fmt"
	"sort"
)

type Scheduler interface {
	Schedule(resource *Resource,pls []plan) ([]SchePlan,error)
	BooK(resource *Resource,num int32) ([]ScheBook,error)
}
type SchePlan struct {
	nodeName string
	plan plan
}
type ScheBook struct {
	nodeName string
	total int32
}
/*轮询*/
type LeastNodeScheduler struct {

}

/*优先给同一个节点*/
type  SameNodeScheduler struct {

}
func (s *SameNodeScheduler) Schedule(resource *Resource,pls []plan) ([]SchePlan,error)  {
	nodes, err := s.choice(resource, int32(len(pls)))
	if err != nil {
		return nil,err
	}
	list := make([]SchePlan,0,len(nodes))
	index := 0;
	for _, node := range nodes {
		for j := int32(0); j < node.total; j++ {
			list = append(list,SchePlan{
				nodeName: node.name,
				plan:   pls[index],
			})
			index++
		}
	}
	return list,nil
}
func (s *SameNodeScheduler) BooK(resource *Resource, num int32) ([]ScheBook, error) {
	nodes, err := s.choice(resource,num)
	if err != nil {
		return nil,err
	}
	list := make([]ScheBook,0,len(nodes))
	for _, node := range nodes {
		list = append(list,ScheBook{
			nodeName: node.name,
			total:    node.total,
		})
	}
	return list,nil
}
func (s SameNodeScheduler)  choice(resource *Resource,num int32) ([]Node,error)  {
	nodes := make([]Node,0,len(resource.nodes))
	total := int32(0)
	for _, nodeResource := range resource.nodes {
		if nodeResource.freeNum >0 {
			nodes = append(nodes,Node{
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
	choiceNodes := make([]Node,0,num)
	index := 0
	for i := 0; i < len(nodes); i++ {
		for nodes[index].total<num{
			index++
			continue
		}
		choiceNodes = append(choiceNodes,Node{
			name:     nodes[index].name,
			priority: 1,
			total:    nodes[index].total,
		})
		return choiceNodes,nil
	}
	for i := len(nodes)-1; i>=0; i++ {
		choiceNodes = append(choiceNodes,Node{
			name:     nodes[i].name,
			priority: 1,
			total:    nodes[i].total,
		})
		if num <= 0 {
			break
		}
	}
	return choiceNodes,nil
}
/*根据优先级分配*/
type PriorityScheduler struct {

}
func (p *PriorityScheduler) Schedule(resource *Resource,pls []plan) ([]SchePlan,error)  {
	nodes, err := p.choice(resource, int32(len(pls)))
	if err != nil {
		return nil,err
	}
	list := make([]SchePlan,0,len(nodes))
	for i, node := range nodes {
		list = append(list,SchePlan{
			nodeName: node.name,
			plan:   pls[i],
		})
	}
	return list,nil
}
func (p *PriorityScheduler) BooK(resource *Resource, num int32) ([]ScheBook, error) {
	nodes, err := p.choice(resource,num)
	if err != nil {
		return nil,err
	}
	list := make([]ScheBook,0,len(nodes))
	for _, node := range nodes {
		list = append(list,ScheBook{
			nodeName: node.name,
			total:    node.total,
		})
	}
	return list,nil
}
func (p PriorityScheduler) choice(resource *Resource,num int32) ([]Node,error)  {
	nodes := make([]Node,0,len(resource.nodes))
	total := int32(0)
	for _, nodeResource := range resource.nodes {
		if nodeResource.freeNum >0 {
			nodes = append(nodes,Node{
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
			choiceNodes = append(choiceNodes,Node{
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
	name string
	priority PLevel
	total int32
}


var resourceScheduler Scheduler
func DeafultScheduler() Scheduler {
	if resourceScheduler == nil {
		resourceScheduler = &LeastNodeScheduler{}
	}
	return resourceScheduler
}
func (l *LeastNodeScheduler) Schedule(resource *Resource,pls []plan) ([]SchePlan,error)  {
	return nil,nil
}
func (l *LeastNodeScheduler) BooK(resource *Resource, num int32) ([]ScheBook, error) {
	return nil,nil
}