<?php

use PHPUnit\Framework\TestCase;

class VideosTest extends TestCase
{

    public $videosIndex;

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->videosIndex = new Roxby\Elastic\Indexes\Videos();
    }

    public function testIndexExist()
    {
        $this->assertTrue($this->videosIndex->indexExists());
    }

    public function testAddSingleDocument()
    {
        $data = [
            'item_id' => 1233,
            'title' => 'kukumuku',
            'content' => 'kukumuku',
            'duration' => 800,
            'tube' => 'test'
        ];
        $res = $this->videosIndex->add($data, 1);
        $this->assertTrue($res['result'] == 'created');
    }

    public function testUpdateSingleDocument()
    {
        $data = [
            'duration' => 1500
        ];
        $res = $this->videosIndex->update($data, 1);
        $this->assertTrue($res['result'] == 'updated');
    }

    public function testBulkAdd()
    {
        $params = [];
        $i = 0;
        while ($i < 5) {
            $params[] = [
                'tube' => 'test',
                'title' => 'lorem ipsum' . $i,
                'content' => 'lorem ipsum dolor sit amet' . $i
            ];
            $i++;

        }
        $res = $this->videosIndex->bulkAdd($params);
        $this->assertTrue($res);
    }

//    public function testSearch()
//    {
//        $incorrect = [
//            'query' => "not existing query",
//            'tube' => "test",
//            'fields' => ["title", "content"]
//        ];
//        $res = $this->videosIndex->search($incorrect);
//        $this->assertTrue(is_null($res));
//
//        $correct = [
//            'query' => "kuku",
//            'tube' => "test",
//            'fields' => ["title", "content"]
//        ];
//        $res = $this->videosIndex->search($correct);
//        $this->assertTrue(!is_null($res));
//    }

    public function testDelete()
    {
        $res = $this->videosIndex->delete(1);
        $this->assertTrue($res['result'] == 'deleted');
    }

    public function testDeleteByQuery()
    {
        $res = $this->videosIndex->deleteByQuery(['title' => 'lorem ipsum']);
        $this->assertTrue(isset($res['deleted']));
        
        $this->assertTrue($res['deleted'] == 5);
    }
}