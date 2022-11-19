package resource

import (
	"fmt"
	"testing"
	"time"
)

func TestSchedule(t *testing.T) {
	resources := NewResource()
	node1 := NewNodeResource("node-1",2)
	node2 := NewNodeResource("node-2",2)
	node3 := NewNodeResource("node-3",3)
	resources.AddNode(node1)
	resources.AddNode(node2)
	resources.AddNode(node3)
	fmt.Println(resources)
	//resources.RemoveNode("node-1")
	//fmt.Println(resources)
	fmt.Println("-------------------------------")
	//scheduler := NewPollingNodeScheduler()
	//scheduler := NewPriorityScheduler()
	//node2.Priority(PLevel(99))
	//node3.Priority(PLevel(98))
	//scheduler := NewLeastNodeScheduler()
	scheduler := NewSameNodeScheduler()
	nodeNames := make([]string,0)
	pls := NewPlans("test-1", func(p plan, node string) error {
		fmt.Printf("####plan:%v,nodeName:%s\n",p,node)
		nodeNames = append(nodeNames,node)
		return nil
	},scheduler)
	fmt.Println(scheduler.strategy)
	fmt.Println(pls.scheduler,"****")
	pls.Append("p1",1)
	pls.Append("p2",2)
	pls.Append("p3",3)
	pls.Append("p4",4)
	pls.Append("p5",5)

	fmt.Println(resources.Schedule(pls))
	fmt.Println(resources)
	fmt.Println("-----------Recycle--------------------")
	for _, name := range nodeNames {
		resources.Recycle(name,1)
	}
	fmt.Println(resources)
	fmt.Println("-----------Reset--------------------")
	resources.Reset()
	fmt.Println(resources)
}
func TestBook(t *testing.T) {
	resources := NewResource()
	node1 := NewNodeResource("node-1",2)
	node2 := NewNodeResource("node-2",2)
	node3 := NewNodeResource("node-3",3)
	resources.AddNode(node1)
	resources.AddNode(node2)
	resources.AddNode(node3)
	//scheduler := NewSameNodeScheduler()
	scheduler := NewPollingNodeScheduler()
	fmt.Println(resources.Book("test-3",time.Duration(time.Second*2),2,scheduler))
	fmt.Println(resources)
	nodeNames := make([]string,0)
	pls := NewPlans("test-3", func(p plan, node string) error {
		fmt.Printf("####plan:%v,nodeName:%s\n",p,node)
		nodeNames = append(nodeNames,node)
		return nil
	},nil)
	pls.Append("p1",1)
	//pls.Append("p2",2)
	fmt.Println("-----------DoBook--------------------")
	fmt.Println(resources.DoBook(pls.bizName,pls))

	fmt.Println(resources)

	time.Sleep(time.Second*3)
	pls2 := NewPlans("test-3", func(p plan, node string) error {
		fmt.Printf("###plan:%v,nodeName:%s\n",p,node)
		nodeNames = append(nodeNames,node)
		return nil
	},nil)
	pls2.Append("p3",3)
	fmt.Println(resources.DoBook("test-3",pls2))
	fmt.Println(resources)
	fmt.Println("-----------Recycle--------------------")
	for _, name := range nodeNames {
		resources.Recycle(name,1)
	}
	fmt.Println(resources)

}
