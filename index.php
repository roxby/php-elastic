<?php

require __DIR__ .'/src/bootstrap.php';


$v = new \App\Indexes\Videos();
//$res = $v->exist();
//var_dump($res);


$params = [
    'tube' => 'analdin',
    'query' => 'teen hardcore',
    'fields' => ["title^3", "cats^10", "tags", "models"]
];
$res = $v->search($params);
var_dump($res);
