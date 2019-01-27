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

    public function testCrud()
    {
        $this->isIndexExist();
        $this->addSingleDocument();
        $this->documentExist();
        $this->updateSingleDocument();
        $this->search();
        $this->deleteSingleDocument();
        $this->bulkAdd();
        $this->deleteByQuery();
    }

    public function isIndexExist()
    {
        $this->assertTrue($this->videosIndex->indexExists());
    }

    public function refresh()
    {
        $this->videosIndex->indexRefresh();
    }

    public function addSingleDocument()
    {
        $data = [
            'video_id' => 123123,
            'title' => 'foxes',
            'description' => 'brown fox jumped over lazy dog',
            'tube' => 'test',
            'post_date' => '2014-01-12 00:00:00'
        ];
        $res = $this->videosIndex->add($data, 1);
        $this->assertTrue($res['result'] == 'created');
        $this->refresh();
    }

    public function documentExist()
    {
        $res = $this->videosIndex->searchOne('test', 123123);
        $this->assertTrue(!empty($res));
    }

    public function updateSingleDocument()
    {
        $data = [
            'post_date' => '2015-01-12 00:00:00'
        ];
        $res = $this->videosIndex->update($data, 1);
        $this->assertTrue($res['result'] == 'updated');
        $this->refresh();
    }

    public function bulkAdd()
    {
        $params = [];
        $i = 0;
        while ($i < 5) {
            $params[] = [
                'video_id' => 12313 . $i,
                'tube' => 'test',
                'title' => 'lorem ipsum' . $i,
                'description' => 'lorem ipsum dolor sit amet' . $i,
                "post_date" => "201$i-01-12 00:00:00"
            ];
            $i++;

        }
        $res = $this->videosIndex->bulkAdd($params);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function search()
    {

        $existingQuery = "brown fox";
        $notExistingQuery = "not existing query";
        $tube = "test";
        $params = [
            'fields' => ["title" => 1, "description" => 3]
        ];

        $res = $this->videosIndex->searchMany($existingQuery, $tube, $params);
        $this->assertTrue(!is_null($res));


        $res = $this->videosIndex->searchMany($notExistingQuery, $tube, $params);
        $this->assertTrue(is_null($res));
    }


    public function deleteSingleDocument()
    {
        $res = $this->videosIndex->delete(1);
        $this->assertTrue($res['result'] == 'deleted');
    }

    public function deleteByQuery()
    {
        $res = $this->videosIndex->deleteByQuery(['title' => 'lorem ipsum']);
        $this->assertTrue(isset($res['deleted']));

        $this->assertTrue($res['deleted'] == 5);
    }
}