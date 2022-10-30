package resources

type Scheduler interface {
	Schedule(resource *Resource,pls *plans) ([]SchePlan,error)
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
type LeastNodeScheduler struct {

}



var resourceScheduler Scheduler
func DeafultScheduler() Scheduler {
	if resourceScheduler == nil {
		resourceScheduler = &LeastNodeScheduler{}
	}
	return resourceScheduler
}
func (l *LeastNodeScheduler) Schedule(resource *Resource,pls *plans) ([]SchePlan,error)  {
	return nil,nil
}
func (l *LeastNodeScheduler) BooK(resource *Resource, num int32) ([]ScheBook, error) {
	return nil,nil
}