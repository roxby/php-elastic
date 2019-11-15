<?php

namespace Roxby\Elastic\Indexes;

class Searches extends AbstractIndex
{
    public $name = "searches";
    const  MIN_ALLOWED_COUNT = 100;

    protected static $instance = null;

    public static function getInstance($hosts = [])
    {
        if (is_null(self::$instance)) {
            self::$instance = new Searches($hosts);
        }
        return self::$instance;
    }


    /**
     * @return array
     */
    public function buildMapping()
    {
        return [
            "query" => [
                "type" => "text"
            ],
            "last_updated" => [
                "type" => "date",
                "format" => "yyyy-MM-dd HH:mm:ss"
            ],
            "count" => [
                "type" => "integer"
            ],
            "tube" => [
                "type" => "keyword"
            ]
        ];
    }


    /**
     * @param array $searchQuery
     * @param array $params possible keys:
     * - size (limit) integer
     * - from(skip) integer
     * - sort - array,
     * @param array $fields - document fields to return
     * @return array
     */
    private function buildRequestBody(array $searchQuery, $params = [], $fields = [])
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($defaults, $params);
        $body = [
            "_source" => $fields, //if not empty - get only specified fields, otherwise get all
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => $searchQuery
        ];
        if (isset($params['sort']) && is_array($params['sort'])) {
            $body['sort'] = $params['sort'];
        }
        return [
            "index" => $this->name,
            "body" => $body
        ];
    }

    /**
     * perform search - get queries related to sent query
     * @param $tube
     * @param $query
     * @param array $params
     * - from integer
     * - size integer
     * @param array $fields
     * @return array|null
     */
    public function searchMany($tube, $query, array $params = [], $fields = [])
    {
        //filter by tube, get related queries, but not the one is sent
        $searchQuery = [
            "bool" => [
                "must" => [
                    "match" => [
                        "query" => $query,
                    ]

                ],
                "filter" => [
                    "term" => ["tube" => $tube]
                ],
                "must_not" => [
                    ["terms" => ["_id" => [$this->generateId($tube, $query)]]] //not current query
                ]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * perform search get most popular searches
     * @param $tube
     * @param array $params
     * @param array $fields
     * @return array|null
     */
    public function getMostPopular($tube, array $params = [], array $fields = [])
    {
        $defaults = [
            "sort" => ["count" => ["order" => "desc"]]
        ];
        $params = array_merge($defaults, $params);
        $searchQuery = [
            "bool" => [
                "filter" => ["term" => ["tube" => $tube]],
                "must" => ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * get randomized queries
     * @param $tube
     * @return array|null
     */
    public function getRandom($tube)
    {
        $searchQuery = [
            "function_score" => [
                "functions" => [
                    ["random_score" => new \stdClass()]
                ],
                "query" => [
                    "bool" => [
                        "must" => [
                            ["term" => ["tube" => $tube]],
                            ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
                        ]
                    ]]
            ]];
        $data = $this->buildRequestBody($searchQuery);
        return $this->search($data);

    }

    private function normalizeQuery($query)
    {
        $query = strtolower($query);
        return trim(str_replace(" ", "_", $query));
    }

    public function upsert($tube, $query)
    {
        $params = [
            "index" => $this->name,
            "type" => "_doc",
            "id" => $this->generateId($tube, $query),
            "body" => [
                "script" => [
                    "source" => "ctx._source.count++; ctx._source.last_updated=params.time;",
                    "params" => ["time" => date("Y-m-d H:i:s")]
                ],
                "upsert" => [
                    "query" => strtolower($query),
                    "tube" => $tube,
                    "count" => 1,
                    "last_updated" => date("Y-m-d H:i:s")
                ]
            ]
        ];
        return $this->update($params);
    }



    public function getOne($tube, $query)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $this->generateId($tube, $query)
        ];
        return $this->get($params);
    }

    public function deleteOne($tube, $query)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $this->generateId($tube, $query)
        ];
        return $this->delete($params);
    }

    protected function generateId($tube, $query)
    {
        $query = $this->normalizeQuery($query);
        $final = $tube . "-" . $query;
        return md5($final);
    }


}
