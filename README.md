# resource

#### 介绍
Resource 将处理任务的资源进行管理、并为任务分配资源。其主要适用： 有N个节点，每个节点处理任务能力不同。当有任务需要处理时，找到一个合适的节点处理。
##### Resouce 实现下列调度算法：
	轮询。
    最少使用,总是将任务分配到资源最多的节点。
    最少节点，尽可能将一组任务分配到同一个节点。
    优先级，优先分配到优先级最高的节点。
##### Resouce 实现功能
    实时调度。
    预订资源调度(预定资源将保留一段时间)
#### 使用说明
##### 实时调度
```
//资源添加
resources := NewResource()
node1 := NewNodeResource("node-1",2)
node2 := NewNodeResource("node-2",2)
node3 := NewNodeResource("node-3",3)
resources.AddNode(node1)
resources.AddNode(node2)
resources.AddNode(node3)

//任务创建
scheduler := NewSameNodeScheduler()
nodeNames := make([]string,0)
pls := NewPlans("test-1", func(p plan, node string) error {
    //Resource 已经找到合适的节点。需要调用者处理分配任务。
    fmt.Printf("####plan:%v,nodeName:%s\n",p,node)
    nodeNames = append(nodeNames,node)
    return nil
},scheduler)
pls.Append("p1",1)
pls.Append("p2",2)
pls.Append("p3",3)
pls.Append("p4",4)
pls.Append("p5",5)

//任务调度
fmt.Println(resources.Schedule(pls))

//节点资源回收
for _, name := range nodeNames {
    resources.Recycle(name,1)
}
```
##### 预定调度
```
//资源添加
resources := NewResource()
node1 := NewNodeResource("node-1",2)
node2 := NewNodeResource("node-2",2)
node3 := NewNodeResource("node-3",3)
resources.AddNode(node1)
resources.AddNode(node2)
resources.AddNode(node3)
//预定资源
scheduler := NewPollingNodeScheduler()
resources.Book("test-3",time.Duration(time.Second*2),2,scheduler)
nodeNames := make([]string,0)
//任务创建
pls := NewPlans("test-3", func(p plan, node string) error {
    fmt.Printf("####plan:%v,nodeName:%s\n",p,node)
    nodeNames = append(nodeNames,node)
    return nil
},nil)
pls.Append("p1",1)
pls.Append("p2",2)
//使用预定资源
resources.DoBook(pls.bizName,pls)
//节点资源回收
for _, name := range nodeNames {
resources.Recycle(name,1)
}
```