<?php

require __DIR__ .'/src/bootstrap.php';


$v = new \App\Indexes\Videos();
//$res = $v->exist();
//var_dump($res);

// Is this for kinda testing purposes?
// BTW (and for the future): this library MUST have full test coverage before we go live with excited
// But this is just FYI

$params = [
    'tube' => 'analdin',
    'query' => 'teen hardcore',
    'fields' => ["title^3", "cats^10", "tags", "models"]
];
$res = $v->search($params);
var_dump($res);
