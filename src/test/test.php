<?php

use Ssdk\Rabbitmq\Clients\RabbitMQClient;

require '../../vendor/autoload.php';


$client = new RabbitMQClient();

$topic = $client->declareTopic('test-topic');

$queue = $client->declareQueue('test-queue');

$client->bindQueue($queue, $topic);

// $client->produce('test message', $queue);

// $client->consume($queue);
$client->subscribeConsumer([$queue]);

