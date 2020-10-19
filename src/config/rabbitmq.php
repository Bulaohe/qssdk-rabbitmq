<?php

return [
    'host' => env('RABBITMQ_HOST', '127.0.0.1'),
    'vhost' => env('RABBITMQ_VHOST', '/'),
    'user' => env('RABBITMQ_USER', 'guest'),
    'pass' => env('RABBITMQ_PASSWORD', 'guest'),
    'port' => env('RABBITMQ_PORT', 5672),
    'queue_api' => env('RABBITMQ_QUEUE_API', 'http://localhost'),
    'log_api' => env('RABBITMQ_LOG_API', 'http://localhost'),
    'log_error_api' => env('RABBITMQ_LOG_ERROR_API', 'http://localhost'),
    'timeout' => env('RABBITMQ_API_TIMEOUT', 5),
    'callerid' => env('RABBITMQ_CALLERID', 'default'),
];