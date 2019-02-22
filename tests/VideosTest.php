<?php

use PHPUnit\Framework\TestCase;
use Roxby\Elastic\Indexes\Videos as VIndex;

class VideosTest extends TestCase
{

    public $videosIndex;
    public $hosts = ['localhost:9200'];
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
        $this->search();
        $this->bulkAdd();
        $this->documentsCount();
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
            'external_id' => 1,
            'title' => 'foxes',
            'description' => 'brown fox jumped over lazy dog',
            'tube' => $this->tube,
            'post_date' => '2014-01-12 00:00:00',
            'duration' => 500
        ];
        $res = $this->videosIndex->add($data);
        $this->assertTrue($res);
        $this->refresh();
    }


    public function getDocument()
    {
        $uid = $this->videosIndex->generateUID($this->tube, 1);
        $res = $this->videosIndex->get($uid);
        $this->assertTrue(!is_null($res));
    }

    public function updateSingleDocument()
    {
        $uid = $this->videosIndex->generateUID($this->tube, 1);
        $data = [
            'post_date' => '2015-01-12 00:00:00'
        ];
        $res = $this->videosIndex->update($data, $uid);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function bulkAdd()
    {
        $bulkIds = [2, 3, 4, 5, 6];
        $params = [];
        for ($i = 0; $i < count($bulkIds); $i++) {
            $vid = $bulkIds[$i];
            $params[] = [
                'external_id' => $vid,
                'tube' => $this->tube,
                'title' => 'lorem ipsum',
                'description' => 'lorem ipsum dolor sit amet',
                "post_date" => "2015-01-12 00:00:00",
                "duration" => 400
            ];
        }
        $res = $this->videosIndex->bulkAdd($params);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function search()
    {
        $existingQuery = "brown fox";
        $notExistingQuery = "not existing query";
        $params = [
            'fields' => ["title" => 1, "description" => 3]
        ];
        $res = $this->videosIndex->searchMany($this->tube, $existingQuery, $params);
        $this->assertTrue(!is_null($res));


        $res = $this->videosIndex->searchMany($this->tube, $notExistingQuery, $params);
        $this->assertTrue(is_null($res));
    }

    public function documentsCount()
    {
        $count = $this->videosIndex->countHavingQuery($this->tube, 'lorem ipsum');
        $this->assertTrue(is_numeric($count));
        $this->assertEquals(5, $count);
    }


    public function deleteSingleDocument()
    {
        $uid = $this->videosIndex->generateUID($this->tube, 1);
        $res = $this->videosIndex->delete($uid);
        $this->assertTrue($res);
        $this->videosIndex->indexRefresh();
    }

    public function bulkDelete()
    {
        $bulkIds = [2, 3, 4, 5, 6];
        $uids = [];
        foreach ($bulkIds as $id) {
            $uids[] = $this->videosIndex->generateUID($this->tube, $id);
        }
        $this->videosIndex->bulkDelete($uids);

        $this->videosIndex->indexRefresh();

        $count = $this->videosIndex->count($this->tube);
        $this->assertEquals(0, $count);
    }

}