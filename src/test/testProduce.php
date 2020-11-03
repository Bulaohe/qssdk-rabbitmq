<?php

use Ssdk\Rabbittest\Produce\ProduceTest;

require '../../vendor/autoload.php';


$test = new ProduceTest();
$test->produceDirect();
$test->produceDirect(100000);

$test->produceTopic();
$test->produceTopic(100000);