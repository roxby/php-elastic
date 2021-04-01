<?php

namespace Roxby\Elastic\Indexes;

use Roxby\Elastic\Response;
class Searches extends AbstractIndex
{
    public $name = "searches";
    const  MIN_ALLOWED_COUNT = 100;

    protected static $instance = null;

    public static function getInstance($hosts = []) :Searches
    {
        if (is_null(self::$instance)) {
            self::$instance = new Searches($hosts);
        }
        return self::$instance;
    }

    public function getSettings(): array
    {
        return [];
    }

    /**
     * @return array
     */
    public function getProps() :array
    {
        return [
            "properties" => [
                "query_en" => [
                    "type" => "text",
                    "fields" => [
                        "keyword" => [
                            "type" => "keyword"
                        ],
                        "english" => [
                            "type" => "text",
                            "analyzer" => "english",
                        ]
                    ]
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
                ],
                "query_de" => [
                    "type" => "text",
                    "analyzer" => "german",
                    "fields" => [
                        "keyword" => [
                            "type" => "keyword"
                        ],
                        "german" => [
                            "type" => "text",
                            "analyzer" => "german",
                        ]
                    ]
                ],
                "query_es" => [
                    "type" => "text",
                    "fields" => [
                        "keyword" => [
                            "type" => "keyword"
                        ],
                        "spanish" => [
                            "type" => "text",
                            "analyzer" => "spanish",
                        ]
                    ]
                ]
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
    private function buildRequestBody(array $searchQuery, array $params = [], array $fields = []) :array
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
     * @return array
     */
    public function getMany(string $tube, string $query, array $params = [], array $fields = []) :array
    {
        $normalized = $this->normalizeQuery($query);
        //filter by tube, get related queries
        $searchQuery = [
            "bool" => [
                "must" => [
                    "multi_match" => [
                        "query" => $normalized,
                        "fields" => ["query_en", "query_en.english"]
                    ]
                ],
                "filter" => [
                    "term" => ["tube" => $tube]
                ]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * search document for specific tube
     * possible only with keyword  field types
     * search for exact match
     * @param $tube string
     * @param $field string
     * @param $value string
     * @return array
     */
    public function getOne(string $tube, string $field, string $value) :array
    {
        $searchQuery = [
            "bool" => [
                "must" => [
                    ["term" => ["tube" => $tube]],
                    ["term" => ["${field}.keyword" => $this->normalizeQuery($value)]]
                ]
            ]
        ];

        $data = $this->buildRequestBody($searchQuery);
        $res = $this->search($data);
        if (isset($res["error"])) return $res;

        if(isset($res["result"]["data"]) && count($res["result"]["data"])) {
            return Response::success($res["result"]["data"][0]);
        }
        return Response::success([]);
    }

    /**
     * get most popular queries
     * @param $tube
     * @param array $params
     * @param array $fields
     * @return array
     */
    public function getMostPopular(string $tube, array $params = [], array $fields = []) :array
    {
        $defaults = [
            "sort" => ["count" => ["order" => "desc"]]
        ];
        $params = array_merge($defaults, $params);

        $searchQuery = [
            "bool" => [
                "filter" => ["term" => ["tube" => $tube]],
                "must" => [
                    ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
                ]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * get randomized queries
     * @param $tube string
     * @param $params array
     * @param $fields array
     * @return array
     */
    public function getRandom(string $tube, array $params = [], array $fields = []) :array
    {
        $mustRule = [
            ["term" => ["tube" => $tube]],
            ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
        ];
        $searchQuery = [
            "function_score" => [
                "functions" => [
                    ["random_score" => new \stdClass()]
                ],
                "query" => [
                    "bool" => [
                        "must" => $mustRule
                    ]]
            ]];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * clean query - allow only alphanumeric and spaces
     * @param $query
     * @return string|string[]|null
     */
    private function normalizeQuery(string $query)
    {
        $query = strtolower($query);
        //allow any letter + any number + whitespace
        return trim(preg_replace('/[^\p{L}\p{N}\s]/u', '', $query));
    }


    private function prepareParams(array $params) :array
    {
        $allowedKeys = array_keys($this->getProps());
        $filtered = array_filter($params, function ($key) use ($allowedKeys) {
            return in_array($key, $allowedKeys);
        }, ARRAY_FILTER_USE_KEY);
        return array_map(function ($value) {
            return $this->normalizeQuery($value);
        }, $filtered);
    }

    /**
     * @param $params array
     * @param $doIncrement boolean
     * @return array
     */
    private function prepareUpdateScript(array $params, bool $doIncrement) :array
    {
        $scriptStr = "ctx._source.last_updated=params.time;";

        if($doIncrement) {
            $scriptStr .= "ctx._source.count++;";
        }
        foreach ($params as $key => $value) {
            $scriptStr .= "if (ctx._source.$key == null) { ctx._source.$key = \"$value\"; }";
        }
        return [
            "source" => $scriptStr,
            "params" => ["time" => date("Y-m-d H:i:s")]
        ];
    }
    /**
     * insert or update document. if exist - only increment counter
     * @param string $tube
     * @param array $params
     * @param boolean $doIncrement define to increment or not document counter
     * @return array
     */
    public function upsert(string $tube, array $params, bool $doIncrement = true) :array
    {
        if (!isset($params["query_en"])) {
            return Response::error(new \Exception("query not sent"));
        }
        //initial data in case document not yet exist
        $data2store = [
            "tube" => $tube,
            "count" => 1,
            "last_updated" => date("Y-m-d H:i:s")
        ];
        $params = $this->prepareParams($params);
        $request = [
            "index" => $this->name,
            "id" => $this->generateId($tube, $params["query_en"]),
            "body" => [
                "script" => $this->prepareUpdateScript($params, $doIncrement),
                "upsert" => array_merge($data2store, $params)
            ]
        ];
        return $this->update($request);
    }

    /**
     * get one document by id
     * @param $tube
     * @param $query
     * @return array
     */
    public function getById(string $tube, string $query) :array
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $query)
        ];
        return $this->get($params);
    }

    /**
     * delete one document by id
     * @param $tube
     * @param $query
     * @return array
     */
    public function deleteOne($tube, $query) :array
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $query)
        ];
        return $this->delete($params);
    }

    /**
     * build document id as hash of normalized query+tube
     * @param $tube
     * @param $query
     * @return string
     */
    protected function generateId(string $tube, string $query) :string
    {
        $query = str_replace(" ", "_", $this->normalizeQuery($query));
        $final = $tube . "_" . $query;
        return md5($final);
    }




}
