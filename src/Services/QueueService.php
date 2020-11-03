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
    public $produceError = '';
    
    /**
     * 
     * @param object $redis
     */
    public function __construct($redis = null)
    {
        $this->config = require dirname(__DIR__) . '/config/rabbitmq.php';;
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
     * @param int $delay milliseconds 毫秒
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
            $this->produceError = $e->getMessage();
            try {
                $response = Http::timeout($this->config['timeout'])->post($url, $params);
            } catch (\Exception $e) {
                $this->produceError = $e->getMessage();
                $this->sendErrorLog($params, $e->getMessage(), 'produce');
                return '';
            }
        } catch (\Error $e) {
            $this->produceError = $e->getMessage();
            Oalog::log('通信故障或超时 - prodce', ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_trace' => $e->getTraceAsString()]);
            return '';
        }
        
        if ($response->isOk()) {
            $arr = $response->json();
            if ($arr['code'] == 0) {
                return $arr['data']['message_id'] ?? '';
            } else {
                $this->produceError = $arr['msg'];
                Oalog::log($arr['msg'], ['msg' => '队列生产服务', 'url' => $url, 'params' => $params]);
                return '';
            }
        }
        
        Oalog::log('通信故障或超时 - prodce', ['msg' => '队列生产服务不健康', 'url' => $url, 'params' => $params]);
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
            $subscriptionConsumer->subscribe($tmpConsumer, function (Message $message, Consumer $consumer) use ($handler, $thisObj, $queue) {
                // process message
                try {
                    try {
                        $headers = $message->getHeaders();
                        $msg_id = $headers['x-msg-id'];
                        $properties = $message->getProperties();
                        $x_max_retry = $message->getProperty('x_max_retry', 2);
                        if (isset($handler['max_try'])) {
                            $x_max_retry = $handler['max_try'];
                        }
                    } catch (\Throwable $e) {
                        $consumer->acknowledge($message);
                        
                        $this->sendErrorLog([
                            'headers' => $headers,
                            'properties' => $properties,
                            'handler' => $handler,
                            'msg' => 'msg_id error',
                        ], $e->getMessage(), 'consume');
                        return true;
                    }
                    
                    $queueName = $queue->getQueueName();
                    
                    $handle_class = $properties['handler_class'] ?? '';
                    $handle_method = $properties['handler_method'] ?? '';
                    $delay = $headers['x-delay'] ?? 0;
                        
                    try {
                        $body = $message->getBody();
                        $body = json_decode($body, true);
                    } catch (\Throwable $e) {
                        $consume_x_max_retry = 1;
                        $consumer->acknowledge($message);
                        // 消息体不是数组，直接进入 6,最终消费失败
                        $thisObj->consumeLog($queueName, $msg_id, $body, 6, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                        return true;
                    }
                    
                    try {
                        $redisKey = 'message_consume_retry_' . $msg_id;
                        if ($thisObj->redis->exists($redisKey)) {
                            $consume_x_max_retry = $thisObj->redis->get($redisKey);
                        } else {
                            $consume_x_max_retry = 0;
                        }
                    
                    } catch (\Throwable $e) {
                        $consume_x_max_retry = ($consume_x_max_retry ?? 0) + 1;
                        $thisObj->redis->set($redisKey, $consume_x_max_retry);
                        // consume Redis 异常 5,消费失败重试
                        $consumer->reject($message, true);
                        $thisObj->consumeLog($queueName, $msg_id, $body, 5, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                        return true;
                    }
                    
                    
                    if ($consume_x_max_retry >= $x_max_retry) {
                        $consumer->acknowledge($message);
                        // consume fail 6,最终消费失败
                        $thisObj->consumeLog($queueName, $msg_id, $body, 6, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                        return true;
                    }
                    
                    // consume start 进入消费 3,出队成功
                    $thisObj->consumeLog($queueName, $msg_id, $body, 3, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                    
                    if (!empty($handler)) {
                        $class = new $handler['handler_class']($body);
                        $class->{$handler['handler_method']}();
                        $consumer->acknowledge($message);
                        // consume success 4 消费成功
                        $thisObj->consumeLog($queueName, $msg_id, $body, 4, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                    } else if (isset($properties['handler_class']) && !empty($properties['handler_class'])) {
                        $class = new $properties['handler_class']($body);
                        $class->{$handler['handler_method']}();
                        $consumer->acknowledge($message);
                        // consume success 4 消费成功
                        $thisObj->consumeLog($queueName, $msg_id, $body, 4, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                    } else {
                        $consume_x_max_retry += 1;
                        $thisObj->redis->set($redisKey, $consume_x_max_retry);
                        $consumer->reject($message, true);
                        // consume 5,消费失败重试
                        $thisObj->consumeLog($queueName, $msg_id, $body, 5, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                    }
                } catch (\Exception $e) {
                    // consume 5,消费失败重试
                    $consume_x_max_retry += 1;
                    $thisObj->redis->set($redisKey, $consume_x_max_retry);
                    $consumer->reject($message, true);
                    $thisObj->consumeLog($queueName, $msg_id, $body, 5, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                } catch (\Error $e) {
                    $consumer->acknowledge($message);
                    // consume fail 6,最终消费失败
                    $thisObj->consumeLog($queueName, $msg_id, $body, 6, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                }
                return true;
            });
        }
        
        // 0 无限消费， 直到发生错误
        // 单位毫秒
        $subscriptionConsumer->consume(0); // 2 sec
    }
    
    private function formatConsumeLog($queueName, $message, $delay, $maxTry, $handleClass, $handleMethod, $status)
    {
        $params = [
            'queue_name' => $queueName,
            'message' => $message,
            'delay' => $delay,
            'max_retry_times' => $maxTry,
            'handler_class' => $handleClass,
            'handler_method' => $handleMethod,
            'status' => $status,
        ];
        
        $params = array_merge($params, $this->getPublicRequestParams());
        
        return $params;
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
    
    public function consumeLog(string $queueName, string $messageId, array $message, int $status, int $tryTime, $delay, $maxTry, $handleClass = '', $handleMethod = '')
    {
        $url = $this->logApi . self::LOG_API_URI;
        
        $params = [
            'queue_name' => $queueName,
            'message' => $message,
            'delay' => $delay,
            'max_retry_times' => $maxTry,
            'handler_class' => $handleClass,
            'handler_method' => $handleMethod,
            'status' => $status,
            'message_id' => $messageId,
            'retry_times' => $tryTime,
        ];
        
        $params = array_merge($params, $this->getPublicRequestParams());
        
        try {
            Http::timeout(5)->post($url, $params);
        } catch (\Throwable $e) {
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
            
            $data = [
                'params' => $params,
                'error_msg' => $errorMsg
            ];
            $data = array_merge($data, $this->getPublicRequestParams());
            
            Http::timeout(5)->post($url, $data);
        } catch (\Throwable $e) {
            Oalog::log('通信故障或超时 - ' . $fromFunction, ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_msg' => $errorMsg]);
        }
    }
}