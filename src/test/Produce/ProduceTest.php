<?php

namespace Ssdk\Rabbittest\Produce;

use Ssdk\Rabbitmq\Services\QueueService;

class ProduceTest
{
    public function produceDirect($delay = 0)
    {
        $redis = new \Redis();
        $redis->connect('127.0.0.1', 6379);
        
        $queueService = new QueueService($redis);
        
        // queue name
        $maxTry = 2;
        $message = [
            'oa_uid' => 7747
        ];
        $queueName = 'create_user';
        
        $queueService->produce($queueName, $message, '', '', $maxTry, 0);
    }
    
    public function produceTopic($delay = 0)
    {
        $redis = new \Redis();
        $redis->connect('127.0.0.1', 6379);
        
        $queueService = new QueueService($redis);
        
        // queue name
        $maxTry = 2;
        $message = [
            'oa_uid' => 7747
        ];
        $queueName = 'user_kpi';
        
        $queueService->produce($queueName, $message, '', '', $maxTry, 0);
    }
}