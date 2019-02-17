<?php

use PHPUnit\Framework\TestCase;

class SearchesTest extends TestCase
{
    public $searchesIndex;
    public $tube = "test";
    public $query_first = "lazy dog";
    public $query_second = "diligent dog";

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->searchesIndex = new Roxby\Elastic\Indexes\Searches(['localhost:9200']);
    }

    public function testCrud()
    {
        $this->isIndexExist();
        $this->addSingleDocument();
        $this->documentExist();
        $this->incrementDocumentCount();
        $this->search();
        $this->deleteByQuery();
    }

    public function isIndexExist()
    {
        $this->assertTrue($this->searchesIndex->indexExists());
    }

    public function refresh()
    {
        $this->searchesIndex->indexRefresh();
    }

    public function addSingleDocument()
    {
        $res = $this->searchesIndex->updateOrCreate($this->tube, $this->query_first);
        $this->assertTrue($res);
        $this->refresh();
    }


    public function documentExist()
    {
        $res = $this->searchesIndex->searchOne($this->tube, $this->query_first);
        $this->assertTrue(!is_null($res));
        $this->assertEquals(1, $res['count']);
    }

    public function incrementDocumentCount()
    {
        $res = $this->searchesIndex->updateOrCreate($this->tube, $this->query_first);
        $this->assertEquals(1, $res);
        $this->refresh();

        $res = $this->searchesIndex->searchOne($this->tube, $this->query_first);
        $this->assertEquals(2, $res['count']);
    }



    public function search()
    {
        $this->searchesIndex->updateOrCreate($this->tube, $this->query_second);
        $this->refresh();
        $res = $this->searchesIndex->searchMany($this->tube, 'dog');
        $this->assertTrue(!is_null($res));
        $this->assertEquals(2, count($res));

    }
    public function deleteByQuery()
    {
        $res = $this->searchesIndex->deleteByQuery($this->tube, $this->query_first);
        $this->assertEquals(1, $res);

        $this->searchesIndex->indexRefresh();
        $res = $this->searchesIndex->deleteByQuery($this->tube, $this->query_second);
        $this->assertEquals(1, $res);

        $this->searchesIndex->indexRefresh();

        $count = $this->searchesIndex->count($this->tube);
        $this->assertEquals(0,$count);
    }
}