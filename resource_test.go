package resources

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
	//pls := NewPlans("test-1",nil,scheduler)
	//fmt.Println(scheduler.strategy)
	//fmt.Println(pls.scheduler,"****")
	//pls.Append("p1",1)
	//pls.Append("p2",2)
	//pls.Append("p3",3)
	//pls.Append("p4",4)
	//pls.Append("p5",5)
	//_,err := resources.Schedule(pls)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Println(resources.Schedule(pls))
	fmt.Println(resources.Book("test-3",time.Duration(time.Second*100),2,scheduler))
	fmt.Println(resources)

	//fmt.Println("-----------Reset--------------------")
	//resources.Reset()
	//fmt.Println(resources)
}
