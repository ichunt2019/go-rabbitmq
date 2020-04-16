package main

import (
	"fmt"
	"github.com/ichunt2019/go-rabbitmq/utils/rabbitmq"
	"time"
)

type RecvPro struct {

}

//// 实现消费者 消费消息失败 自动进入延时尝试  尝试3次之后入库db
/*
返回值 error 为nil  则表示该消息消费成功
否则消息会进入ttl延时队列  重复尝试消费3次
3次后消息如果还是失败 消息就执行失败  进入告警 FailAction
 */
func (t *RecvPro) Consumer(dataByte []byte) error {
	//time.Sleep(500*time.Microsecond)
	//return errors.New("顶顶顶顶")
	fmt.Println(string(dataByte))
	time.Sleep(1*time.Second)
	//return errors.New("顶顶顶顶")
	return nil
}

//消息已经消费3次 失败了 请进行处理
/*
如果消息 消费3次后 仍然失败  此处可以根据情况 对消息进行告警提醒 或者 补偿  入库db  钉钉告警等等
 */
func (t *RecvPro) FailAction(dataByte []byte) error {
	fmt.Println(string(dataByte))
	fmt.Println("任务处理失败了，我要进入db日志库了")
	fmt.Println("任务处理失败了，发送钉钉消息通知主人")
	return nil
}



func main() {
	t := &RecvPro{}



	//rabbitmq.Recv(rabbitmq.QueueExchange{
	//	"a_test_0001",
	//	"a_test_0001",
	//	"",
	//	"",
	//	"amqp://guest:guest@192.168.2.232:5672/",
	//},t,5)

	/*
		runNums: 表示任务并发处理数量  一般建议 普通任务1-3    就可以了
	 */
	rabbitmq.Recv(rabbitmq.QueueExchange{
		"a_test_0001",
		"a_test_0001",
		"hello_go",
		"direct",
		"amqp://guest:guest@192.168.2.232:5672/",
	},t,3)



}