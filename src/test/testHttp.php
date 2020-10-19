<?php

use Ssdk\Rabbitmq\Clients\Http;

require '../../vendor/autoload.php';

$response = Http::timeout(0.4)->get('http://dev.ams.2345.cn/health');

$status = $response->status();
// int
var_dump($status);

$isOk = $response->isOk();
// true / false
var_dump($isOk);

$rs = $response->json();
var_dump($rs);