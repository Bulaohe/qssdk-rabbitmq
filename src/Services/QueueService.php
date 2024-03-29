<?php

namespace Ssdk\Rabbitmq\Services;

use Ssdk\Rabbitmq\Clients\Http;
use Ssdk\Oalog\Facades\Oalog;
use Ssdk\Rabbitmq\Clients\RabbitMQClient;
use Interop\Queue\Consumer;
use Interop\Queue\Message;
use Ssdk\Oalog\Logger;

class QueueService
{
    //生产队列消息接口地址
    const PRODUCE_API_URI = '/queue/produce';

    //生产队列重试消息接口地址
    const PRODUCE_RE_API_URI = '/queue/reProduce';
    
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
    protected $daleySecs = [
        1 => 30000,
        2 => 600000,
        3 => 1800000,
        4 => 3600000,
        5 => 7200000,
        6 => 21600000,
    ];
    
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
                Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_trace' => $e->getTraceAsString()], Logger::ERROR);
                return '';
            }
        } catch (\Error $e) {
            $this->produceError = $e->getMessage();
            Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_trace' => $e->getTraceAsString()], Logger::ERROR);
            return '';
        }
        
        if ($response->isOk()) {
            $arr = $response->json();
            if ($arr['code'] == 0) {
                return $arr['data']['message_id'] ?? '';
            } else {
                $this->produceError = $arr['msg'];
                Oalog::log($arr['msg'], ['msg' => '消息总线队列生产服务', 'url' => $url, 'params' => $params]);
                return '';
            }
        }
        
        Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => '队列生产服务不健康', 'url' => $url, 'params' => $params], Logger::ERROR);
        return '';
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
    public function reProduce(string $message_id, string $queueName, array $message, string $handleClass = '', string $handleMethod = '', int $maxTry = 5, int $delay  = 0): string
    {
        $url = $this->queueApi . self::PRODUCE_RE_API_URI;
        $params = [
            'queue_name' => $queueName,
            'message' => $message,
            'message_id' => $message_id,
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
                Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_trace' => $e->getTraceAsString()], Logger::ERROR);
                return '';
            }
        } catch (\Error $e) {
            $this->produceError = $e->getMessage();
            Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_trace' => $e->getTraceAsString()], Logger::ERROR);
            return '';
        }
        
        if ($response->isOk()) {
            $arr = $response->json();
            if ($arr['code'] == 0) {
                return $arr['data']['message_id'] ?? '';
            } else {
                $this->produceError = $arr['msg'];
                Oalog::log($arr['msg'], ['msg' => '消息总线队列生产服务', 'url' => $url, 'params' => $params]);
                return '';
            }
        }
        
        Oalog::log('消息总线通信故障或超时 - prodce', ['msg' => '队列生产服务不健康', 'url' => $url, 'params' => $params], Logger::ERROR);
        return '';
    }

    /**
     * 订阅消费
     * @param array $queues 队列列表 \Interop\Amqp\Impl\AmqpQueue
     * @param array $handler handle_class, handler_method
     */
    public function consume(array $queues, array $handler = [])
    {
        try {
            $rabbitClient = new RabbitMQClient();
            $context = $rabbitClient->context;
            
            $subscriptionConsumer = $context->createSubscriptionConsumer();
            $thisObj = $this;
            foreach ($queues as $queue) {
                $tmpConsumer = $context->createConsumer($queue);
                $subscriptionConsumer->subscribe($tmpConsumer, function (Message $message, Consumer $consumer) use ($handler, $thisObj, $queue) {
                    // get valid reis obj again
                    try {
                        if (isset($handler['redis_class']) && isset($handler['redis_method'])) {
                            $this->redis = (new $handler['redis_class'])->$handler['redis_method']($this->redis);
                        }
                    } catch (\Throwable $e) {
                        // do nothing
                    }
                    
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
                            
                            // error 级别发送到预警
                            $this->logError('消费，获取消息ID错误' . $e->getMessage(), [
                                'headers' => $headers,
                                'properties' => $properties,
                                'handler' => $handler,
                                'msg' => 'msg_id error'
                            ], Logger::ERROR);
                            
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
                            // error 级别发送到预警
                            $this->logError($e->getMessage() . " | message_id : {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'message_id' => $msg_id, 'trace' => $e->getTraceAsString()], Logger::ERROR);
                            $this->sendErrorLog([
                                'headers' => $headers,
                                'properties' => $properties,
                                'handler' => $handler,
                                'msg' => 'get body error',
                            ], $e->getMessage(), 'consume');
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
                            $this->logError($e->getMessage(), ['queue_name' => $queueName, 'message' => $body, 'trace' => $e->getTraceAsString()]);
                            $this->sendErrorLog([
                                'headers' => $headers,
                                'properties' => $properties,
                                'handler' => $handler,
                                'msg' => 'redis error',
                            ], $e->getMessage(), 'consume');
                            return true;
                        }
                        
                        
                        if ($consume_x_max_retry >= $x_max_retry) {
                            $consumer->acknowledge($message);
                            $thisObj->redis->del($redisKey);
                            // consume fail 6,最终消费失败
                            $thisObj->consumeLog($queueName, $msg_id, $body, 6, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                            // error 级别发送到预警
                            $this->logError('重试最终消费失败' . " | message_id : {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'message_id' => $msg_id], Logger::INFO);
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
                        $consumer->acknowledge($message);
                        $thisObj->consumeLog($queueName, $msg_id, $body, 5, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                        $delay = $this->daleySecs[$consume_x_max_retry] ?? 60000;//如果存在配置取配置
                        $thisObj->reProduce($msg_id, $queueName, $body, "", "", $x_max_retry, $delay);
                        if ($consume_x_max_retry == 1) {
                            $this->logError('消费异常 | errormsg : ' . $e->getMessage() . " | message_id : {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'message_id' => $msg_id , 'trace' => $e->getTraceAsString()], Logger::ERROR);
                        } else {
                            $this->logError('消费异常 | errormsg : ' . $e->getMessage() . " | message_id : {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'message_id' => $msg_id , 'trace' => $e->getTraceAsString()]);
                        }
                        $this->sendErrorLog([
                            'queue_name' => $queueName,
                            'message' => $body,
                            'handler' => $handler,
                            'msg' => 'consume exception error',
                        ], $e->getMessage(), 'consume');
                        
                    } catch (\Error $e) {
                        $consumer->acknowledge($message);
                        // consume fail 6,最终消费失败
                        $thisObj->consumeLog($queueName, $msg_id, $body, 6, $consume_x_max_retry, $delay, $x_max_retry, $handle_class, $handle_method);
                        // error 级别发送到预警
                        $code = $e->getCode();
                        if ($code >= 1000000 && $code < 2000000) {
                            $this->logError($e->getMessage() . " | message_id: {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'trace' => $e->getTraceAsString()], Logger::INFO);
                        } else {
                            $this->logError($e->getMessage() . " | message_id: {$msg_id}", ['queue_name' => $queueName, 'message' => $body, 'trace' => $e->getTraceAsString()], Logger::ERROR);
                        }
                        $this->sendErrorLog([
                            'queue_name' => $queueName,
                            'message' => $body,
                            'handler' => $handler,
                            'msg' => 'consume fatal error',
                        ], $e->getMessage(), 'consume');
                    }
                    return true;
                });
            }
            
            // 0 无限消费， 直到发生错误
            // 单位毫秒
            $subscriptionConsumer->consume(0); // 2 sec
        } catch (\Throwable $e) {
            $this->logError($e->getMessage(), ['trace' => $e->getTraceAsString()]);
        }
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
            Oalog::log('通信故障或超时 - ' . $fromFunction, ['msg' => $e->getMessage(), 'url' => $url, 'params' => $params, 'error_msg' => $errorMsg], Logger::ERROR);
        }
    }
    
    private function logError($errorMsg, $data, $level = Logger::INFO)
    {
        Oalog::log($errorMsg, $data, $level);
    }
}