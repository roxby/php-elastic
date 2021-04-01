<?php

use PHPUnit\Framework\TestCase;
use Roxby\Elastic\Indexes\Blacklist as BIndex;
class BlackList extends TestCase
{


    public $blackListIndex;
    public $hosts = ['localhost:9200'];
    public $tube = "phpunit_test";
    public $query_first = "lazy dog";
    public $query_second = "diligent dog";

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->blackListIndex = BIndex::getInstance($this->hosts);
    }

    public function testCrud()
    {
        $this->isIndexExist();
        $this->addSingleDocument();
        $this->bulkAdd();
        $this->checkSimilarities();
        $this->manipulateOneDocument();
    }

    public function isIndexExist()
    {
        $res = BIndex::exists($this->hosts, 'blacklist');
        if (!$res["result"]) {
            $this->blackListIndex->create();
            $res = BIndex::exists($this->hosts, 'blacklist');
        }
        $this->assertTrue($res["success"]);
        $this->assertTrue($res["result"]);
    }
    
    public function addSingleDocument() 
    {
        $data = $this->blackListIndex->addOne("lola");
        $this->assertTrue($data["success"]);
        $this->assertEquals(1, $data["result"]);
        $this->refresh();
    }

    public function bulkAdd()
    {

        $terms = ["lola", "lolita","underage","virgin" ,"little girl", "forced" ,"rape", "rapist"];
        $res = $this->blackListIndex->addMany($terms);
        $this->assertTrue($res["success"]);

        //count-1 cause first term  already added before
        $this->assertEquals(count($terms) - 1, $res["result"]);
        $this->refresh();
    }


    public function manipulateOneDocument()
    {
        $data = $this->blackListIndex->getMany("little", true);
        $this->assertTrue($data["success"]);

        $this->assertEquals(1, $data["result"]["total"]);

        $docId = $data["result"]["data"][0]["id"] ?? null;

        $data = $this->blackListIndex->updateOne($docId, "little white girl");
        $this->assertTrue($data["success"]);
        $this->assertEquals(1, $data["result"]);


        $data = $this->blackListIndex->deleteOne($docId);
        $this->assertTrue($data["success"]);
        $this->assertEquals(1, $data["result"]);


    }


    public function checkSimilarities()
    {
        $data = $this->blackListIndex->hasSimilarTerms("virgins");
        $this->assertTrue($data["success"]);
        $this->assertTrue($data["result"]);


        $data = $this->blackListIndex->hasSimilarTerms("lesbian");
        $this->assertTrue($data["success"]);
        $this->assertFalse($data["result"]);
    }

    public function refresh()
    {
        $this->blackListIndex->indexRefresh();
    }
}