<?php

use PHPUnit\Framework\TestCase;

class SearchesTest extends TestCase
{
    public $searchesIndex;

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
        $this->updateSingleDocument();
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
        $res = $this->searchesIndex->updateOrCreate('test', 'lazy dog');
        $this->assertEquals('created', $res['result']);
        $this->refresh();
    }


    public function documentExist()
    {
        $res = $this->searchesIndex->searchOne('test', 'lazy dog');
        $this->assertTrue(!is_null($res));
        $this->assertEquals(1, $res[0]['count']);
    }

    public function updateSingleDocument()
    {
        $res = $this->searchesIndex->updateOrCreate('test', 'lazy dog');
        $this->assertTrue($res == 1);
        $this->refresh();

        $res = $this->searchesIndex->searchOne('test', 'lazy dog');

        $this->assertEquals(2, $res[0]['count']);
    }



    public function search()
    {
        $this->searchesIndex->updateOrCreate('test', 'diligent dog');
        $this->refresh();
        $res = $this->searchesIndex->searchMany('test', 'dog');
        $this->assertTrue(!is_null($res));
        $this->assertEquals(2, count($res));

    }
    public function deleteByQuery()
    {
        $res = $this->searchesIndex->deleteByQuery('test', ['query' => 'dog']);
        $this->assertTrue(isset($res['deleted']));

        $this->assertEquals(2, $res['deleted']);
    }
}