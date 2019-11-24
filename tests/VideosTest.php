<?php

use PHPUnit\Framework\TestCase;
use Roxby\Elastic\Indexes\Videos as VIndex;

class VideosTest extends TestCase
{

    public $videosIndex;
    public $hosts = ['localhost:9200'];
    public $initExternalId = 10000000;

    public $tube = "test";

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->videosIndex = VIndex::getInstance($this->hosts);
    }

    public function testCrud()
    {
        $this->isIndexExist();
        $this->addSingleDocument();
        $this->updateSingleDocument();
        $this->getDocument();
        $this->bulkAdd();
        $this->search();
        $this->deleteSingleDocument();
        $this->bulkDelete();
    }

    public function isIndexExist()
    {
        $res = VIndex::exists($this->hosts, 'videos');
        $this->assertTrue($res);
    }

    public function refresh()
    {
        $this->videosIndex->indexRefresh();
    }

    public function addSingleDocument()
    {
        $data = [
            'external_id' => $this->initExternalId,
            'title' => 'foxes',
            'description' => 'brown fox jumped over lazy dog',
            'tube' => $this->tube,
            'post_date' => date('2014-01-12 00:00:00'),
            'duration' => 500
        ];
        $res = $this->videosIndex->addOne($data);

        $this->assertTrue($res);
        $this->refresh();
    }


    public function getDocument()
    {
        $res = $this->videosIndex->getById($this->tube, $this->initExternalId);
        $this->assertTrue(!is_null($res));
    }

    public function updateSingleDocument()
    {
        $data = [
            'post_date' => date('2015-01-12 00:00:00')
        ];
        $res = $this->videosIndex->updateOne($data, $this->tube, $this->initExternalId);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function bulkAdd()
    {
       $bulkIds = $this->generateExternalIds(5);
        $params = [];
        for ($i = 0; $i < count($bulkIds); $i++) {
            $vid = $bulkIds[$i];
            $params[] = [
                'external_id' => $vid,
                'tube' => $this->tube,
                'title' => 'lorem ipsum',
                'description' => 'lorem ipsum dolor sit amet',
                "post_date" => date("2015-01-12 00:00:00"),
                "duration" => 400
            ];
        }
        $res = $this->videosIndex->addMany($params);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function search()
    {
        //find one
        $existingQuery = "brown fox";
        $notExistingQuery = "not existing query";

        $res = $this->videosIndex->getMany($this->tube, $existingQuery);
        $this->assertTrue(!empty($res['data']));
        $this->assertEquals(1, $res['total']);


        //find 0
        $res = $this->videosIndex->getMany($this->tube, $notExistingQuery);
        $this->assertTrue(empty($res['data']));
        $this->assertEquals(0, $res['total']);

        //find 5
        $res = $this->videosIndex->getMany($this->tube, 'lorem ipsum');
        $this->assertTrue(!empty($res['data']));
        $this->assertEquals(5, $res['total']);

    }


    public function deleteSingleDocument()
    {
        $res = $this->videosIndex->deleteOne($this->tube, $this->initExternalId);
        $this->assertTrue($res);
        $this->videosIndex->indexRefresh();
    }

    public function bulkDelete()
    {
        $bulkIds = $this->generateExternalIds(5);
        $this->videosIndex->deleteMany($this->tube, $bulkIds);

        $this->videosIndex->indexRefresh();

        $count = $this->videosIndex->count(["term" => ["tube" => $this->tube]]);
        $this->assertEquals(0, $count);
    }


    private function generateExternalIds($count)
    {
        $initialId = $this->initExternalId;
        $bulkIds = [];
        for($i = 1; $i <= $count; $i++) {
            $bulkIds[] = $initialId + $i;
        }
        return $bulkIds;
    }
}
