<?php

use Ssdk\Rabbitmq\Clients\RabbitMQClient;
use Ssdk\Rabbitmq\Services\QueueService;

require '../../vendor/autoload.php';


$client = new RabbitMQClient();

$topic = $client->declareTopic('test-topic');

$queue = $client->declareQueue('test-queue');

$client->bindQueue($queue, $topic);


$client->consume($queue);


$redis = new Redis();
$redis->connect('127.0.0.1', 16379);


$queueService = new QueueService($redis);


// queue name
$maxTry = 2;
$message = [
    'oa_uid' => 7747
];
$queueName = 'test-queue';

$ps = $queueService->produce($queueName, $message, $maxTry);

var_dump($ps);

// $params = [
//     'oa_uid' => 7747
// ];

// $properties = [
//     'x_max_retry' => 2,
// ];

// $headers = [
//     'x-msg-id' => 'sdfk34020340skfk3840923',
// ];

// $client->produce(json_encode($params, JSON_UNESCAPED_UNICODE), $queue, $properties, $headers);

// // $client->consume($queue);
// $client->subscribeConsumer([$queue]);

