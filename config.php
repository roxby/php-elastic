<?php
$dotenv = new Dotenv\Dotenv(__DIR__);
$dotenv->load();

return  [
    'hosts' => [
            'host' => getenv('ELASTIC_HOST', 'localhost'),
    ]
];