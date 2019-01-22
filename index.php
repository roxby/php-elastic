<?php

require __DIR__ .'/src/bootstrap.php';


$v = new Roxby\Elastic\Indexes\Videos();
$res = $v->indexExists();
//var_dump($res);
//die();
// Is this for kinda testing purposes?
// BTW (and for the future): this library MUST have full test coverage before we go live with excited
// But this is just FYI

$params = [
    'tube' => 'test',
    'query' => 'kukumuk',
    //'fields' => ["title^3", "cats^10", "tags", "models"]
];
$res = $v->search($params);
var_dump($res);
