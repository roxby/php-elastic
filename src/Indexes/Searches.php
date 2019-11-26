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
    public function getProps()
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
            ],
            "query_de" => [
                "type" => "text",
                "analyzer" => "german",
                "fields" => [
                    "raw" => [
                        "type" => "keyword"
                    ]
                ]
            ],
            "query_es" => [
                "type" => "text",
                "analyzer" => "spanish",
                "fields" => [
                    "raw" => [
                        "type" => "keyword"
                    ]
                ]
            ],
            "query_ru" => [
                "type" => "text",
                "analyzer" => "russian",
                "fields" => [
                    "raw" => [
                        "type" => "keyword"
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
     * - lang string (possible values - en, de, es, ru)
     * @param array $fields
     * @return array|null
     */
    public function getMany($tube, $query, array $params = [], $fields = [])
    {
        //if language params exist prop name should be "query_{lang}", otherwise, just query - default english version
        $queryProp = isset($params["lang"]) ? $this->getTranslateQueryName($params["lang"]) : "query";
        //filter by tube, get related queries, but not the one is sent
        $searchQuery = [
            "bool" => [
                "must" => ["match" => [$queryProp => $query]],
                "filter" => [
                    "term" => ["tube" => $tube]
                ],
//                "must_not" => [
//                    ["terms" => ["_id" => [$this->generateId($tube, $query)]]] //not current query
//                ]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery, $params, $fields);
        return $this->search($data);
    }

    /**
     * search document with parametrized field=>value pair fro specific tube
     * possible only with keyword or integer field types
     * search for exact match
     * @param $tube string
     * @param $field string
     * @param $value string
     * @return array|null
     */
    public function getOne($tube, $field, $value)
    {
        $searchQuery = [
            "bool" => [
                "must" => [
                    ["term" => ["tube" => $tube]],
                    ["term" => [$field => $value]]
                ]
            ]
        ];
        $data = $this->buildRequestBody($searchQuery);
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

        $mustRule = [
            ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
        ];
        if(isset($params["lang"])) {
            $mustRule[] = ["exists" => ["field" => $this->getTranslateQueryName($params["lang"])]];
        }

        $searchQuery = [
            "bool" => [
                "filter" => ["term" => ["tube" => $tube]],
                "must" => $mustRule
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
     * @return array|null
     */
    public function getRandom($tube, $params = [], $fields = [])
    {
        $mustRule = [
            ["term" => ["tube" => $tube]],
            ["range" => ["count" => ["gte" => self::MIN_ALLOWED_COUNT]]]
        ];

        if(isset($params["lang"])) {
            $mustRule[] = ["exists" => ["field" => $this->getTranslateQueryName($params["lang"])]];
        }
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
    private function normalizeQuery($query)
    {
        $query = strtolower($query);
        //allow any letter + any number + whitespace
        return trim(preg_replace('/[^\p{L}\p{N}\s]/u', '', $query));
    }

    /**
     * insert or update document. if exist - only increment counter
     * @param $tube
     * @param $query
     * @param array $params
     * @return bool
     */
    public function upsert($tube, $query, $params = [])
    {
        //build initial data 2 store in case document not yet exist
        $data2store = [
            "query" => $this->normalizeQuery($query),
            "tube" => $tube,
            "count" => 1,
            "last_updated" => date("Y-m-d H:i:s")
        ];

        $allowedProps = $this->getProps();
        //loop through sent data, normalize each text field and add to data
        foreach ($params as $key => $value) {
            if(isset($allowedProps[$key]) && $allowedProps[$key]["type"] === "text") {
                $data2store[$key] = $this->normalizeQuery($value);
            }
        }
        $params = [
            "index" => $this->name,
            "id" => $this->generateId($tube, $query),
            "body" => [
                "script" => [
                    "source" => "ctx._source.count++; ctx._source.last_updated=params.time;",
                    "params" => ["time" => date("Y-m-d H:i:s")]
                ],
                "upsert" => $data2store
            ]
        ];
        return $this->update($params);
    }


    /**
     * get one document by id
     * @param $tube
     * @param $query
     * @return array|null
     */
    public function getById($tube, $query)
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $query)
        ];
        return $this->get($params);
    }

    public function deleteOne($tube, $query)
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
    protected function generateId($tube, $query)
    {
        $query = str_replace(" ", "_", $this->normalizeQuery($query));
        $final = $tube . "-" . $query;
        return md5($final);
    }

    /**
     * @param $lang
     * @return string|null
     */
    private function getTranslateQueryName($lang)
    {
        $langs = ["es", "de", "ru"];
        if(!in_array($lang, $langs)) {
            return null;
        }
        return "query_${lang}";
    }


}
