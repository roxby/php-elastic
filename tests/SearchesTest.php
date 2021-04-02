<?php

use PHPUnit\Framework\TestCase;
use Roxby\Elastic\Indexes\Searches as SIndex;
class SearchesTest extends TestCase
{
    public $searchesIndex;
    public $hosts = ['localhost:9200'];
    public $tube = "phpunit_test";
    public $query_first = "lazy dog";
    public $query_second = "diligent dog";

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->searchesIndex = SIndex::getInstance($this->hosts);
    }

    public function testCrud()
    {
        $this->isIndexExist();
        $this->addSingleDocument();
        $this->documentExist();
        $this->incrementDocumentCount();
        $this->search();
        $this->delete();
    }

    public function isIndexExist()
    {
        $res = SIndex::exists($this->hosts, 'searches');
        if (!$res["result"]) {
            $this->searchesIndex->create();
            $res = SIndex::exists($this->hosts, 'searches');
        }
        $this->assertTrue($res["success"]);
        $this->assertTrue($res["result"]);
    }

    public function refresh()
    {
        $this->searchesIndex->indexRefresh();
    }

    public function addSingleDocument()
    {
        $data = $this->searchesIndex->upsert($this->tube, ["query_en" => $this->query_first]);
        $this->assertTrue($data["success"]);
        $this->assertEquals(1, $data["result"]);
        $this->refresh();
        $this->refresh();
    }


    public function documentExist()
    {
        $data = $this->searchesIndex->getById($this->tube, $this->query_first);
        $this->assertTrue($data["success"]);
        $this->assertNotEmpty($data["result"]);
    }

    public function incrementDocumentCount()
    {
        $data = $this->searchesIndex->upsert($this->tube, ["query_en" => $this->query_first]);
        $this->assertTrue($data["success"]);
        $this->assertEquals(1, $data["result"]);
        $this->refresh();


        $data = $this->searchesIndex->getById($this->tube, $this->query_first);
        $this->assertNotEmpty($data["result"]);
        $this->assertEquals(2, $data["result"]['count']);
    }



    public function search()
    {
        $this->searchesIndex->upsert($this->tube, ["query_en" => $this->query_second]);
        $this->refresh();
        $data = $this->searchesIndex->getMany($this->tube, 'dog');

        $this->assertNotEmpty($data['result']['data']);
        $this->assertEquals(2, $data['result']['total']);
    }

    public function delete()
    {
        $data = $this->searchesIndex->deleteOne($this->tube, $this->query_first);
        $this->assertTrue($data["success"]);

        $data = $this->searchesIndex->deleteOne($this->tube, $this->query_second);
        $this->assertTrue($data["success"]);

        $this->searchesIndex->indexRefresh();

        $params = [
            'index' => "searches",
            'body' => [
                'query' => [
                    "term" => ["tube" => $this->tube]
                ]
            ]
        ];
        $res = $this->searchesIndex->count($params);
        $this->assertEquals(0,$res["result"]);
    }
}
