<?php

namespace Roxby\Elastic\Indexes;

use Roxby\Elastic\Response;

class Blacklist extends AbstractIndex
{
    public $name = 'blacklist';
    protected static $instance = null;

    public static function getInstance($hosts = []) :Blacklist
    {
        if (is_null(self::$instance)) {
            self::$instance = new Blacklist($hosts);
        }
        return self::$instance;
    }

    /** build index settings array
     * @return array
     */
    public function getSettings(): array
    {
        return [
            "settings" => [
                "analysis" => [
                    "analyzer" => [
                        "sb_analyzer" => ["tokenizer" => "whitespace", "filter" => [ "stemmer" ], "language" => "english"]
                    ]
                ]
            ]
        ];
    }


    /**
     * build index fields mapping array
     * @return array
     */
    public function getProps(): array
    {
        return [
            "properties" => [
                "id" => [
                    "type" => "text",
                ],
                "term" => [
                    "type" => "text",
                    "analyzer" => "sb_analyzer",
                    "search_analyzer" => "sb_analyzer",
                    "fields" => [
                        "keyword" => [
                            "type" => "keyword"
                        ]
                    ]
                ]
            ]
        ];
    }

    /**
     * check if index contain similar term to sent param
     * @param $term
     * @return array
     */
    public function hasSimilarTerms($term) :array
    {
        $data = [
            "index" => $this->name,
            "body" => [
                "query" => ["fuzzy" => ["term" => $term]]
            ]
        ];
        $result =  $this->search($data);
        if(isset($result["error"])) return $result;

        $total = $this->getTotal($result);
        return Response::success($total > 0);
    }

    /**
     * check if index contains exact term
     * @param $value
     * @return bool
     */
    private function exist($value) :bool
    {
        $params = [
            "index" => $this->name,
            "body" =>  [
                "query" => ["term" => ["term" => ["value" => $value, "boost"=> 1.0]]]
            ]
        ];
        $result =  $this->search($params);
        $total = $this->getTotal($result);
        return $total == 1;
    }

    private function getTotal($data) :int
    {
        if($data["success"] && isset($data["result"]) && isset($data["result"]["total"])) {
            return $data["result"]["total"];
        }
        return 0;
    }

    /**
     * insert one document
     * @param $term
     * @return array
     */
    public function addOne($term) :array
    {
        if(!$this->exist($term)) {
            $query = [
                'index' => $this->name,
                'body' => ["term" => $term],
            ];
            return $this->add($query);
        }
        return Response::success(0);
    }


    /**
     * insert many documents
     * @param $terms
     * @return array|false
     */
    public function addMany($terms)
    {
        if (!count($terms)) return false;
        $data = [];
        foreach ($terms as $t) {
            if($this->exist($t)) continue;

            $meta = ["index" => ["_index" => $this->name]];
            array_push($data, $meta, ["term" => $t]);
        }
        return $this->bulkAdd($data);
    }


    /**
     * update document by id
     * @param $id
     * @param $term
     * @return array
     */
    public function updateOne($id, $term) :array
    {
        $params = [
            'index' => $this->name,
            'id' => $id,
            'retry_on_conflict' => 3,
            'body' => [
                'doc' => ["term" => $term]
            ]
        ];
        return $this->update($params);
    }

    /**
     * delete document by id
     * @param $id
     * @return array
     */
    public function deleteOne($id) :array
    {
        $params = [
            'index' => $this->name,
            'id' => $id
        ];
        return $this->delete($params);
    }
}
