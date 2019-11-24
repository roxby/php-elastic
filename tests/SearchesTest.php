<?php

use PHPUnit\Framework\TestCase;
use Roxby\Elastic\Indexes\Searches as SIndex;
class SearchesTest extends TestCase
{
    public $searchesIndex;
    public $hosts = ['localhost:9200'];
    public $tube = "test";
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
        $this->assertTrue($res);
    }

    public function refresh()
    {
        $this->searchesIndex->indexRefresh();
    }

    public function addSingleDocument()
    {
        $res = $this->searchesIndex->upsert($this->tube, $this->query_first);
        $this->assertTrue($res);
        $this->refresh();
    }


    public function documentExist()
    {
        $res = $this->searchesIndex->getById($this->tube, $this->query_first);
        $this->assertTrue(!is_null($res));
        $this->assertEquals(1, $res['count']);
    }

    public function incrementDocumentCount()
    {
        $res = $this->searchesIndex->upsert($this->tube, $this->query_first);
        $this->assertEquals(1, $res);
        $this->refresh();

        $res = $this->searchesIndex->getById($this->tube, $this->query_first);
        $this->assertEquals(2, $res['count']);
    }



    public function search()
    {
        $this->searchesIndex->upsert($this->tube, $this->query_second);
        $this->refresh();
        $res = $this->searchesIndex->getMany($this->tube, 'dog');
        //var_dump($res);
        //$this->assertTrue(!empty($res['data']));
        //$this->assertEquals(2, $res['total']);

    }
    public function delete()
    {
        $res = $this->searchesIndex->deleteOne($this->tube, $this->query_first);
        $this->assertTrue($res);

        $res = $this->searchesIndex->deleteOne($this->tube, $this->query_second);
        $this->assertTrue($res);

        $this->searchesIndex->indexRefresh();

        $count = $this->searchesIndex->count(["term" => ["tube" => $this->tube]]);
        $this->assertEquals(0,$count);
    }
}
