package rabbitmq

import (
	//"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)


// 定义全局变量,指针类型
var mqConn *amqp.Connection
var mqChan *amqp.Channel

// 定义生产者接口
type Producer interface {
	MsgContent() string
}

// 定义生产者接口
type RetryProducer interface {
	MsgContent() string
}

// 定义接收者接口
type Receiver interface {
	Consumer([]byte)    error
	FailAction([]byte)  error
}

// 定义RabbitMQ对象
type RabbitMQ struct {
	connection *amqp.Connection
	Channel *amqp.Channel
	dns string
	QueueName   string            // 队列名称
	RoutingKey  string            // key名称
	ExchangeName string           // 交换机名称
	ExchangeType string           // 交换机类型
	producerList []Producer
	retryProducerList []RetryProducer
	receiverList []Receiver
}

// 定义队列交换机对象
type QueueExchange struct {
	QuName  string           // 队列名称
	RtKey   string           // key值
	ExName  string           // 交换机名称
	ExType  string           // 交换机类型
	Dns     string			  //链接地址
}



// 链接rabbitMQ
func (r *RabbitMQ)MqConnect() (err error){

	mqConn, err = amqp.Dial(r.dns)
	r.connection = mqConn   // 赋值给RabbitMQ对象

	if err != nil {
		fmt.Printf("关闭mq链接失败  :%s \n", err)
	}

	return
}

// 关闭mq链接
func (r *RabbitMQ)CloseMqConnect() (err error){

	err = r.connection.Close()
	if err != nil{
		fmt.Printf("关闭mq链接失败  :%s \n", err)
	}
	return
}

// 链接rabbitMQ
func (r *RabbitMQ)MqOpenChannel() (err error){
	mqConn := r.connection
	r.Channel, err = mqConn.Channel()
	//defer mqChan.Close()
	if err != nil {
		fmt.Printf("MQ打开管道失败:%s \n", err)
	}
	return err
}

// 链接rabbitMQ
func (r *RabbitMQ)CloseMqChannel() (err error){
	r.Channel.Close()
	if err != nil {
		fmt.Printf("关闭mq链接失败  :%s \n", err)
	}
	return err
}




// 创建一个新的操作对象
func NewMq(q QueueExchange) RabbitMQ {
	return RabbitMQ{
		QueueName:q.QuName,
		RoutingKey:q.RtKey,
		ExchangeName: q.ExName,
		ExchangeType: q.ExType,
		dns:q.Dns,
	}
}

func (mq *RabbitMQ) sendMsg (body string)  {
	err :=mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}

	defer mq.Channel.Close()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}


	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	if mq.ExchangeName != "" && mq.RoutingKey != ""{
		err = mq.Channel.Publish(
			mq.ExchangeName,     // exchange
			mq.RoutingKey, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	}else{
		err = mq.Channel.Publish(
			"",     // exchange
			mq.QueueName, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	}

}


func (mq *RabbitMQ) sendRetryMsg (body string,retry_nums int32,args ...string)  {
	err :=mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}
	defer mq.Channel.Close()

	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}

	//原始路由key
	oldRoutingKey := args[0]
	//原始交换机名
	oldExchangeName := args[1]

	table := make(map[string]interface{},3)
	table["x-dead-letter-routing-key"] = oldRoutingKey
	if oldExchangeName != "" {
		table["x-dead-letter-exchange"] = oldExchangeName
	}else{
		mq.ExchangeName = ""
		table["x-dead-letter-exchange"] = ""
	}

	table["x-message-ttl"] = int64(20000)

	//fmt.Printf("%+v",table)
	//fmt.Printf("%+v",mq)
	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, table)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	header := make(map[string]interface{},1)

	header["retry_nums"] = retry_nums + int32(1)

	var ttl_exchange string
	var ttl_routkey string

	if(mq.ExchangeName != "" ){
		ttl_exchange = mq.ExchangeName
	}else{
		ttl_exchange = ""
	}


	if mq.RoutingKey != "" && mq.ExchangeName != ""{
		ttl_routkey = mq.RoutingKey
	}else{
		ttl_routkey = mq.QueueName
	}

	//fmt.Printf("ttl_exchange:%s,ttl_routkey:%s \n",ttl_exchange,ttl_routkey)
	err = mq.Channel.Publish(
		ttl_exchange,     // exchange
		ttl_routkey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:header,
		})
	if err != nil {
		fmt.Printf("MQ任务发送失败:%s \n", err)

	}

}


// 监听接收者接收任务 消费者
func (mq *RabbitMQ) ListenReceiver(receiver Receiver,routineNum int) {
	err :=mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}
	defer mq.Channel.Close()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}


	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}
	// 获取消费通道,确保rabbitMQ一个一个发送消息
	err =  ch.Qos(1, 0, false)
	msgList, err :=  ch.Consume(mq.QueueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Consume err :%s \n", err)
	}
	for msg := range msgList {
		retry_nums,ok := msg.Headers["retry_nums"].(int32)
		if(!ok){
			retry_nums = int32(0)
		}
		// 处理数据
		err := receiver.Consumer(msg.Body)
		if err!=nil {
			//消息处理失败 进入延时尝试机制
			if retry_nums < 3{
				fmt.Println(string(msg.Body))
				fmt.Printf("消息处理失败 消息开始进入尝试  ttl延时队列 \n")
				retry_msg(msg.Body,retry_nums,QueueExchange{
						mq.QueueName,
						mq.RoutingKey,
						mq.ExchangeName,
						mq.ExchangeType,
						mq.dns,
					})
			}else{
				//消息失败 入库db
				fmt.Printf("消息处理3次后还是失败了 入库db 钉钉告警 \n")
				receiver.FailAction(msg.Body)
			}
			err = msg.Ack(true)
			if err != nil {
				fmt.Printf("确认消息未完成异常:%s \n", err)
			}
		}else {
			// 确认消息,必须为false
			err = msg.Ack(true)

			if err != nil {
				fmt.Printf("消息消费ack失败 err :%s \n", err)
			}
		}

	}
}

//消息处理失败之后 延时尝试
func retry_msg(msg []byte,retry_nums int32,queueExchange QueueExchange){
	//原始队列名称 交换机名称
	oldQName := queueExchange.QuName
	oldExchangeName := queueExchange.ExName
	oldRoutingKey := queueExchange.RtKey
	if oldRoutingKey == "" || oldExchangeName == ""{
		oldRoutingKey = oldQName
	}

	if queueExchange.QuName != "" {
		queueExchange.QuName = queueExchange.QuName + "_retry_3";
	}

	if queueExchange.RtKey != "" {
		queueExchange.RtKey = queueExchange.RtKey + "_retry_3";
	}else{
		queueExchange.RtKey = queueExchange.QuName + "_retry_3";
	}

//fmt.Printf("%+v",queueExchange)

	mq := NewMq(queueExchange)
	mq.MqConnect()

	defer func(){
		mq.CloseMqConnect()
	}()
	//fmt.Printf("%+v",queueExchange)
	mq.sendRetryMsg(string(msg),retry_nums,oldRoutingKey,oldExchangeName)


}


func Send(queueExchange QueueExchange,msg string){
	mq := NewMq(queueExchange)
	mq.MqConnect()

	defer func(){
		mq.CloseMqConnect()
	}()
	mq.sendMsg(msg)

}

/*
runNums  开启并发执行任务数量
 */
func Recv(queueExchange QueueExchange,receiver Receiver,runNums int){
	mq := NewMq(queueExchange)
	mq.MqConnect()

	defer func(){
		mq.CloseMqConnect()
	}()

	forever := make(chan bool)
	for i:=1;i<=runNums;i++{
		go func(routineNum int) {
			defer mq.Channel.Close()
			// 验证链接是否正常
			mq.MqOpenChannel()
			mq.ListenReceiver(receiver,routineNum)
		}(i)
	}
	<-forever
}


type retryPro struct {
	msgContent   string
}












