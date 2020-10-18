<?php

use Ssdk\Rabbitmq\Clients\RabbitMQClient;
use Ssdk\Rabbitmq\Services\QueueService;

require '../../vendor/autoload.php';


// $client = new RabbitMQClient();
// $topic = $client->declareTopic('test-topic');
// $queue = $client->declareQueue('test-queue');
// $client->bindQueue($queue, $topic);
// $client->consume($queue);

$client = new RabbitMQClient();
$queue = $client->declareQueue('create_user');
// $topic = $client->declareTopic('create_user');
// $client->bindQueue($queue, $topic);
// $client->consume($queue);

$redis = new Redis();
$redis->connect('127.0.0.1', 6379);

$queueService = new QueueService($redis);

$handlers = [
    'hander_class' => '\Ssdk\Rabbittest\Consume\ConsumeTest',
    'hander_method' => 'handle',
];

$queueService->consume([$queue], $handlers);

