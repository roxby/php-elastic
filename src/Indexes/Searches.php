<?php

namespace Roxby\Elastic\Indexes;

class Searches extends AbstractIndex
{
    public $name = "searches";

    /**
     * @return array
     */
    public function getIndexMapping()
    {
        return [
            "query" => [
                "type" => "text"
            ],
            "alias" => [
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
     * perform search
     * build query filtered by tube name, should match search query
     * @param $tube
     * @param $query
     * @param array $params
     * - from integer
     * - size integer
     * @return array|null
     */
    public function searchMany($tube, $query, array $params = [])
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($defaults, $params);
        $body = [
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => [
                "bool" => [
                    "must" => [
                        "match" => [
                            "query" => $query,
                        ]

                    ],
                    "filter" => [
                        "term" => ["tube" => $tube]
                    ],
                ]
            ],
            "sort" => [
                "last_updated" => [
                    "order" => "desc"
                ]
            ]

        ];
        $data = [
            "index" => $this->name,
            "body" => $body
        ];
        return $this->search($data);
    }

    /**
     * Update document if exist,  otherwise create new
     * @param $tube
     * @param $query
     * @return bool
     */
    public function updateOrCreate($tube, $query)
    {
        $res = $this->updateByQuery($tube, $query);
        if (!$res) {
            $params = [
                "tube" => $tube,
                "query" => $query,
                "alias" => $this->normalizeQuery($query),
                "count" => 1,
                "last_updated" => date("Y-m-d H:i:s")
            ];
            return $this->add($params);
        }
        return $res;

    }

    /**
     * build query for finding exact one document
     * @param $tube
     * @param $query
     * @return array
     */
    private function findOneQuery($tube, $query)
    {
        return [
            "query" => [
                "bool" => [
                    "must" => [
                        ["term" => ["tube" => $tube]],
                        ["term" => ["alias" => $this->normalizeQuery($query)]]
                    ]
                ]
            ]];
    }

    private function normalizeQuery($query)
    {
        return str_replace(" ", "_", $query);
    }

    /**
     * find document, update its counter and set last_updated to now
     * @param $tube
     * @param $query
     * @return int
     */
    private function updateByQuery($tube, $query)
    {
        $body = $this->findOneQuery($tube, $query);
        $body["script"] = [
            "inline" => "ctx._source.count++; ctx._source.last_updated=params['time'];",
            "params" => ["time" => date("Y-m-d H:i:s")]
        ];
        $params = [
            "index" => $this->name,
            "type" => "_doc",
            "body" => $body

        ];
        $res = $this->client->updateByQuery($params);

        return isset($res["updated"]) ? $res["updated"] : 0;

    }

    /**
     * perform search, get one document
     * @param $tube
     * @param $query
     * @return array|null
     */
    public function searchOne($tube, $query)
    {
        $body = $this->findOneQuery($tube, $query);
        $data = [
            "index" => $this->name,
            "type" => "_doc",
            "body" => $body
        ];
        $res = $this->search($data);
        return $res && count($res) ? $res[0] : null;
    }

    /**
     * @param $tube
     * @param $query
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
     * @return integer
     */
    public function deleteByQuery($tube, $query)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'body' => [
                "query" => [
                    "bool" => [
                        "must" => [
                            "match" => ["alias" => $this->normalizeQuery($query)]
                        ],
                        "filter" => [
                            "term" => ["tube" => $tube]
                        ],
                    ]
                ]
            ]
        ];
        $res = $this->client->deleteByQuery($params);
        return isset($res["deleted"]) ? $res["deleted"] : 0;
    }

}