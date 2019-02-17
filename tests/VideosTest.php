<?php

use PHPUnit\Framework\TestCase;

class VideosTest extends TestCase
{

    public $videosIndex;

    public $singleVideoId = 1;
    public $bulkIds = [2, 3, 4, 5, 6];

    public $tube = "test";

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->videosIndex = new Roxby\Elastic\Indexes\Videos(['localhost:9200']);
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
        $this->deleteDocuments();
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
        $uid = $this->buildUid();

        $data = [
            'video_id' => $this->singleVideoId,
            'title' => 'foxes',
            'description' => 'brown fox jumped over lazy dog',
            'tube' => $this->tube,
            'post_date' => '2014-01-12 00:00:00',
            'duration' => 500
        ];
        $res = $this->videosIndex->add($data, $uid);
        $this->assertTrue($res);
        $this->refresh();
    }


    public function getDocument()
    {
        $uid = $this->buildUid();
        $res = $this->videosIndex->get($uid);
        $this->assertTrue(!is_null($res));
        $this->assertEquals($this->singleVideoId, $res["video_id"]);
        $this->assertEquals($this->tube, $res["tube"]);
    }

    public function updateSingleDocument()
    {
        $uid = $this->buildUid();
        $data = [
            'post_date' => '2015-01-12 00:00:00'
        ];
        $res = $this->videosIndex->update($data, $uid);
        $this->assertTrue($res);
        $this->refresh();
    }

    public function bulkAdd()
    {
        $params = [];
        for ($i = 0; $i < count($this->bulkIds); $i++) {
            $vid = $this->bulkIds[$i];
            $params[] = [
                'video_id' => $vid,
                'tube' => $this->tube,
                'title' => 'lorem ipsum',
                'description' => 'lorem ipsum dolor sit amet',
                "post_date" => "2015-01-12 00:00:00",
                "duration" => 400,
                "uid" => $this->buildUid($vid)
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

    public function deleteDocuments()
    {
        //delete first added document
        $uid = $this->buildUid();
        $res = $this->videosIndex->delete($uid);
        $this->assertTrue($res);

        //delete 5 bulk added documents
        for ($i = 0; $i < count($this->bulkIds); $i++) {
            $uid = $this->buildUid($this->bulkIds[$i]);
            $this->videosIndex->delete($uid);
        }
        $this->videosIndex->indexRefresh();

        $count = $this->videosIndex->count($this->tube);
        $this->assertEquals(0, $count);



    }

    private function buildUid($vid = null)
    {
        if (!$vid) {
            $vid = $this->singleVideoId;
        }
        return $this->tube . "-" . $vid;
    }

}