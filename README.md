## go get github.com/ichunt2019/go-rabbitmq

**发送消息**

```
package main

import (
	"fmt"
	_ "fmt"
	"github.com/ichunt2019/go-rabbitmq/utils/rabbitmq"
)

func main() {


	for i := 1;i<10;i++{
		body := fmt.Sprintf("{\"order_id\":%d}",i)
		fmt.Println(body)

		/**
			使用默认的交换机
			如果是默认交换机
			type QueueExchange struct {
			QuName  string           // 队列名称
			RtKey   string           // key值
			ExName  string           // 交换机名称
			ExType  string           // 交换机类型
			Dns     string			  //链接地址
			}
			如果你喜欢使用默认交换机
			RtKey  此处建议填写成 RtKey 和 QuName 一样的值
		 */

		//queueExchange := rabbitmq.QueueExchange{
		//	"a_test_0001",
		//	"a_test_0001",
		//	"",
		//	"",
		//	"amqp://guest:guest@192.168.2.232:5672/",
		//}

		/*
		 使用自定义的交换机
		 */
		queueExchange := rabbitmq.QueueExchange{
			"a_test_0001",
			"a_test_0001",
			"hello_go",
			"direct",
			"amqp://guest:guest@192.168.2.232:5672/",
		}

		rabbitmq.Send(queueExchange,body)


	}


}

```






**消费消息**



```
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

```


**说明：**


```
rabbitmq.Recv(rabbitmq.QueueExchange{
		"a_test_0001",
		"a_test_0001",
		"hello_go",
		"direct",
		"amqp://guest:guest@192.168.2.232:5672/",
	},t,3)
```



第一个参数 QueueExchange说明

```

	
// 定义队列交换机对象
type QueueExchange struct {
	QuName  string           // 队列名称
	RtKey   string           // key值
	ExName  string           // 交换机名称
	ExType  string           // 交换机类型
	Dns     string			  //链接地址
}

```


第二个参数 type Receiver interface说明

| Consumer | FailAction |
| ------- | ------- |
|拿到消息后，用户可以处理任务，如果消费成功 返回nil即可，如果处理失败，返回一个自定义error即可         |      由于消息内部自带消息失败尝试3次机制，3次如果失败后就没必要一直存储在mq，所以此处扩展，可以用作消息补偿和告警     |





```
// 定义接收者接口
type Receiver interface {
	Consumer([]byte)    error
	FailAction([]byte)  error
}
```


第三个参数：runNusm


| runNusm |
| ------- | 
|   消息并发数，同时可以处理多少任务 普通任务 设置为1即可   需要并发的设置成3-5即可      |



























