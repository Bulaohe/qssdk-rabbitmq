<?php

namespace Ssdk\Rabbitmq\Services;

use Ssdk\Rabbitmq\Clients\Http;
use Ssdk\Oalog\Facades\Oalog;
use Ssdk\Rabbitmq\Clients\RabbitMQClient;
use Interop\Queue\Consumer;
use Interop\Queue\Message;

class QueueService
{
    //生产队列消息接口地址
    const PRODUCE_API_URI = '/queue/produce';
    
    //记录消息日志接口地址
    const LOG_API_URI = '/queue/log';
    
    //记录消息日志接口地址
    const LOG_API_ERROR_URI = '/queue/error';
    
    protected $config;
    protected $queueApi;
    protected $logApi;
    protected $logErrorApi;
    protected $redis;
    
    /**
     * 
     * @param object $redis
     */
    public function __construct($redis = null)
    {
        $this->config = require '../config/rabbitmq.php';
        $this->queueApi = $this->config['queue_api'];
        $this->logApi = $this->config['log_api'];
        $this->logErrorApi = $this->config['log_error_api'];
        $this->redis = $redis;
    }
    
    /**
     * call remote produce api
     * 
     * @param string $queueName
     * @param array $message
     * @param string $handleClass
     * @param string $handleMethod
     * @param int $maxTry
     * @param int $delay second
     * @return string message_id 失败返回空字符串
     */
    public function produce(string $queueName, array $message, string $handleClass = '', string $handleMethod = '', int $maxTry = 5, int $delay  = 0): string
    {
        $url = $this->queueApi . self::PRODUCE_API_URI;
        $params = [
            'queue_name' => $queueName,
            'message' => $message,
            'delay' => $delay,
            'max_retry_times' => $maxTry,
            'handler_class' => $handleClass,
            'handler_method' => $handleMethod,
        ];
        
        $params = array_merge($params, $this->getPublicRequestParams());
        
        //生产允许重试一次
        try {
            $response = Http::timeout($this->config['timeout'])->post($url, $params);
        } catch (\Exception $e) {
            try {
                $response = Http::timeout($this->config['timeout'])->post($url, $params);
            } catch (\Exception $e) {
                $this->sendErrorLog($params, $e->getMessage(), 'produce');
                return '';
            }
        }
        
        if ($response->isOk()) {
            $arr = $response->json();
            if ($arr['code'] == 0) {
                return $arr['data']['message_id'] ?? '';
            }
        }
        
        return '';
    }
    
    /**
     * 订阅消费
     * @param array $queues 队列列表 \Interop\Amqp\Impl\AmqpQueue
     * @param array $handler handle_class, handler_method
     */
    public function consume(array $queues, array $handler = [])
    {
        $rabbitClient = new RabbitMQClient();
        $context = $rabbitClient->context;
        
        $subscriptionConsumer = $context->createSubscriptionConsumer();
        $thisObj = $this;
        foreach ($queues as $queue) {
            $tmpConsumer = $context->createConsumer($queue);
            $subscriptionConsumer->subscribe($tmpConsumer, function (Message $message, Consumer $consumer) use ($handler, $thisObj) {
                // process message
                // TODO
                try {
                    $headers = $message->getHeaders();
                    $msg_id = $headers['x-msg-id'];
                    $properties = $message->getProperties();
                    $body = $message->getBody();
                    $x_max_retry = $message->getProperty('consume_x_max_retry', 1);
                    $consume_x_max_retry = $message->getProperty('x_max_retry', 2);
                    if ($consume_x_max_retry >= $x_max_retry) {
                        $consumer->acknowledge($message);
                        
                        // consume fail 5,消费失败
                        $thisObj->consumeLog($msg_id, 5, $x_max_retry);
                    }
                    
                    // consume start 3,进入消费
                    $thisObj->consumeLog($msg_id, 3, $x_max_retry);
                    
                    if (!empty($handler)) {
                        $class = new $handler['handle_class']($body);
                        $class->{$handler['handler_method']}();
                        $consumer->acknowledge($message);
                        // consume success 4 消费成功
                        $thisObj->consumeLog($msg_id, 4, $x_max_retry);
                    } else if (isset($properties['handle_class']) && !empty($properties['handle_class'])) {
                        $class = new $properties['handle_class']($body);
                        $class->{$handler['handler_method']}();
                        $consumer->acknowledge($message);
                        // consume success 4 消费成功
                        $thisObj->consumeLog($msg_id, 4, $x_max_retry);
                    } else {
                        $x_max_retry = $message->getProperty('consume_x_max_retry', 1) + 1;
                        $message->setProperty('x_max_retry', $x_max_retry);
                        // consume 5,消费失败
                        $thisObj->consumeLog($msg_id, 5, $x_max_retry);
                    }
                } catch (\Exception $e) {
                    // consume start 5,消费失败
                    $thisObj->consumeLog($msg_id, 5, $x_max_retry);
                }
                return true;
            });
        }
        
        // 0 无限消费， 直到发生错误
        // 单位毫秒
        $subscriptionConsumer->consume(0); // 2 sec
    }
    
    public function cancelConsume()
    {
        
    }
    
    public function getLogger()
    {
        
    }
    
    public function getRedis()
    {
        
    }
    
    public function consumeLog(string $messageId, int $status, int $tryTime)
    {
        $url = $this->logApi . self::LOG_API_URI;
        
        $params = [
            'message_id' => $messageId,
            'status' => $status,
            'try_time' => $tryTime,
        ];
        
        try {
            Http::timeout(3)->post($url, ['params' => $params]);
        } catch (\Exception $e) {
            $this->sendErrorLog($params, $e->getMessage(), 'consumeLog');
        }
    }
    
    private function getCallerid()
    {
        return $this->config['callerid'] ?? 'default';
    }
    
    /**
     * 获取公共请求参数
     *
     * @return array
     */
    private function getPublicRequestParams()
    {
        return [
            'callerid' => $this->getCallerid(), //场景
            'uuid' => $this->generateUuid(), //请求uuid标识
        ];
    }
    
    /**
     * 生成uuid，不保证全局唯一
     *
     * @return string
     */
    private function generateUuid()
    {
        return $this->getCallerid() . md5(uniqid(mt_rand()) . microtime());
    }
    
    private function sendErrorLog(array $params, string $errorMsg, string $fromFunction = ''):void
    {
        $url = $this->logErrorApi . self::LOG_API_ERROR_URI;
        try {
            Http::timeout(3)->post($url, ['params' => $params, 'error_msg' => $errorMsg]);
        } catch (\Exception $e) {
            Oalog::log('通信故障或超时 - ' . $fromFunction, ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_msg' => $errorMsg]);
        }
    }
}