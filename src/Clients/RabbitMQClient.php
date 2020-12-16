<?php

namespace Ssdk\Rabbitmq\Clients;

use Enqueue\AmqpBunny\AmqpConnectionFactory;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\Impl\AmqpBind;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;
use Interop\Queue\Consumer;
use Interop\Queue\Message;

class RabbitMQClient
{
    
    public $context;
    public $config;
    
    public function __construct()
    {
        $this->config = require dirname(__DIR__) . '/config/rabbitmq.php';
        $this->context = $this->createContext($this->config);
    }
    
    public function createContext($config)
    {
        // connect to AMQP broker at example.com
        $factory = new AmqpConnectionFactory([
            'host' => $config['host'],
            'port' => $config['port'],
            'vhost' => $config['vhost'],
            'user' => $config['user'],
            'pass' => $config['pass'],
            'persisted' => true,
        ]);
        
        return $factory->createContext();
    }
    
    public function declareTopic(string $topic)
    {
        $fooTopic = $this->context->createTopic($topic);
        $fooTopic->setType(AmqpTopic::TYPE_TOPIC);
        $fooTopic->addFlag(AmqpTopic::FLAG_DURABLE);
        $this->context->declareTopic($fooTopic);
        
        return $fooTopic;
    }
    
    public function declareDirect(string $topic)
    {
        $fooTopic = $this->context->createTopic($topic);
        $fooTopic->setType(AmqpTopic::TYPE_DIRECT);
        $fooTopic->addFlag(AmqpTopic::FLAG_DURABLE);
        $this->context->declareTopic($fooTopic);
        
        return $fooTopic;
    }
    
    public function deleteTopic(string $topic)
    {
        $fooTopic = $this->context->createTopic($topic);
        $fooTopic->setType(AmqpTopic::TYPE_DIRECT);
        $fooTopic->addFlag(AmqpTopic::FLAG_DURABLE);
        $this->context->deleteTopic($fooTopic);
        
        return $fooTopic;
    }
    
    public function declareQueue(string $queueName)
    {
        $fooQueue = $this->context->createQueue($queueName);
        $fooQueue->addFlag(AmqpQueue::FLAG_DURABLE);
        $this->context->declareQueue($fooQueue);
        
        return $fooQueue;
    }
    
    public function deleteQueue(string $queueName)
    {
        $fooQueue = $this->context->createQueue($queueName);
        $fooQueue->addFlag(AmqpQueue::FLAG_DURABLE);
        $this->context->deleteQueue($fooQueue);
    }
    
    public function bindQueue($fooQueue, $fooTopic)
    {
        $this->context->bind(new AmqpBind($fooTopic, $fooQueue));
    }
    
    /**
     * json msg
     * @param string $message
     * @param \Interop\Amqp\Impl\AmqpQueue | \Interop\Amqp\Impl\AmqpTopic $destination
     */
    public function produce(string $message, $destination, array $properties = [], array $headers = [])
    {
        $headers = array_merge($headers, ['delivery-mode'=>2]);
        $message = $this->context->createMessage($message, $properties, $headers);
        
        $this->context->createProducer()->send($destination, $message);
    }
    
    /**
     * json msg
     * @param string $message
     * @param \Interop\Amqp\Impl\AmqpQueue $fooQueue
     * @param int $fooTopic 过期时间，毫秒
     */
    public function produceExpiration(string $message, $fooQueue, $expiration)
    {
        $message = $this->context->createMessage($message, [], ['delivery-mode'=>2]);
        
        $this->context->createProducer()->setTimeToLive($expiration)->send($fooQueue, $message);
    }
    
    /**
     * 
     * @param string $message
     * @param \Interop\Amqp\Impl\AmqpQueue | \Interop\Amqp\Impl\AmqpTopic $destination 延时队列
     * @param int $delay 延时时间，milliseconds 毫秒
     */
    public function produceDelayed(string $message, $destination, $delay, array $properties = [], array $headers = [])
    {
        $headers = array_merge($headers, ['delivery-mode'=>2]);
        $message = $this->context->createMessage($message, $properties, $headers);
        
        $this->context->createProducer()
        ->setDelayStrategy(new RabbitMqDlxDelayStrategy())
        ->setDeliveryDelay($delay)->send($destination, $message);
    }
    
    /**
     * this is example
     * @param mixed $fooQueue
     */
    public function consume($fooQueue)
    {
        $consumer = $this->context->createConsumer($fooQueue);
        $message = $consumer->receive();
        
        // process a message
        // TODO
        
        var_dump($message->getBody());
        var_dump($message->getHeaders());
        var_dump($message->getProperties());
        
        $message->setProperty('x_max_retry', 5);
        
//         $consumer->acknowledge($message);
        // $consumer->reject($message);
    }
    
    /**
     * 订阅消费
     * 
     * this is example
     * 
     * @param array $queues 队列列表 \Interop\Amqp\Impl\AmqpQueue
     * @param array $handler handle_class, handler_method
     */
    public function subscribeConsumer(array $queues, array $handler = [])
    {
        
        $subscriptionConsumer = $this->context->createSubscriptionConsumer();
        foreach ($queues as $queue) {
            $tmpConsumer = $this->context->createConsumer($queue);
            $subscriptionConsumer->subscribe($tmpConsumer, function (Message $message, Consumer $consumer) {
                // process message
                // TODO
                
                $consumer->acknowledge($message);
                
                return true;
            });
        }
        
        // 0 无限消费， 直到发生错误
        // 单位毫秒
        $subscriptionConsumer->consume(0); // 2 sec
    }
    
    /**
     * 删除队列
     * @param string $queueName
     */
    public function purgeQueue(string $queueName)
    {
        $queue = $this->context->createQueue($queueName);
        $this->context->purgeQueue($queue);
    }
}