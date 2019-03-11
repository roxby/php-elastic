<?php

namespace Roxby\Elastic\Indexes;

class Videos extends AbstractIndex
{
    public $name = 'videos';
    protected static $instance = null;

    public $fields = [
        "title^3",
        "title.english^3",
        "cats^10",
        "cats.english^10",
        "tags",
        "tags.english",
        "models",
        "models.english"
    ];

    public static function getInstance($hosts = [])
    {
        if (is_null(self::$instance)) {
            self::$instance = new Videos($hosts);
        }
        return self::$instance;
    }

    /**
     * indexing text fields twice: once with the english analyzer and once with the standard analyzer.
     * @see https://qbox.io/blog/elasticsearch-english-analyzer-customize
     * @return array
     */
    public function buildMapping()
    {
        return [
            'external_id' => [
                'type' => 'integer',
            ],
            'title' => [
                'type' => 'text',
                "fields" => $this->addEnglishAnalyzer()

            ],
            'description' => [
                'type' => 'text',
                "fields" => $this->addEnglishAnalyzer()
            ],
            'duration' => [
                'type' => 'integer'
            ],
            'rating' => [
                'type' => 'integer'
            ],
            'video_viewed' => [
                'type' => 'integer'
            ],
            'post_date' => [
                'type' => 'date',
                'format' => "yyyy-MM-dd HH:mm:ss"
            ],
            'models' => [
                'type' => 'text',
                "fields" => $this->addEnglishAnalyzer()
            ],
            'cats' => [
                'type' => 'text',
                "fields" => $this->addEnglishAnalyzer()
            ],
            'tags' => [
                'type' => 'text',
                "fields" => $this->addEnglishAnalyzer()
            ],
            'tube' => [
                'type' => 'keyword'
            ],
            'deleted' => [
                'type' => 'boolean'
            ],
            'url' => [
                'type' => 'text'
            ],
            'thumb' => [
                'type' => 'text'
            ],
            'vthumb' => [
                'type' => 'object'
            ]
        ];
    }

    private function addEnglishAnalyzer()
    {
        return [
            "english" => [
                "type" => 'text',
                "analyzer" => "english"
            ]
        ];
    }

    /**
     * perform search
     * build query filtered by tube name, duration range
     * must match search query, + possible add boost to certain fields
     * @param $tube string ["analdin', 'xozilla', 'vintagetube']
     * @param $query string - search query
     * @param $params
     * - from integer
     * - size integer
     * - fields assoc array of search fields [field1 => 1, field2 => 3, field3 => 10]
     * - min integer - minimum duration
     * - max integer - maximum duration
     * @param array $fields
     * @return array
     */

    public function searchMany($tube, $query, array $params = [], $fields = [])
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($defaults, $params);

        $mustRule = $this->buildMustRule($query, $params);
        $body = [
            "_source" => $fields, //if empty just get all
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => [
                "bool" => [
                    "must" => $mustRule,
                    "filter" => ["term" => ["tube" => $tube]],
                    "must_not" => ["match" => ["deleted" => true]],
                ]
            ],
            "sort" => [
                "post_date" => [
                    "order" => "desc"
                ]
            ]
        ];
        $data = [
            'index' => $this->name,
            'body' => $body
        ];
        return $this->search($data);
    }


    /**
     * Build array of search fields
     * Add additional english field (for english analyzer) to every text field for better
     * @param array $fields - expected structure [field1 => (int)boost, field2 => (int) boost, ...]
     * @return array
     */
    private function buildSearchFields(array $fields)
    {
        $result = [];
        static $mapping = null;
        if (is_null($mapping)) {
            $mapping = $this->getMapping();
        }
        foreach ($fields as $field => $boost) {
            if (isset($mapping[$field])) {
                $result[] = "$field^$boost";
                if ($mapping[$field]['type'] == 'text') {
                    $result[] = "$field.english^$boost";
                }
            }
        }
        return $result;
    }

    /**
     * build must rule to add to search query
     * Query must match to at least one of the fields (sent or defaults)
     * Also if duration params are sent - must search within duration range
     * @param $query string
     * @param $params array
     * - fields assoc array of search fields [field1 => 1, field2 => 3, field3 => 10]
     * - min integer - minimum duration
     * - max integer - maximum duration
     * @return array
     */
    private function buildMustRule($query, $params = [])
    {

        $fieldsArr = isset($params["fields"]) ? $this->buildSearchFields($params['fields']) : $this->fields;
        $mustRule = [
            [
                "multi_match" => [
                    "query" => $query,
                    "fields" => $fieldsArr
                ]

            ]
        ];
        if (!empty($params) && (isset($params['min']) || isset($params['max']))) {
            $range = [
                "range" => [
                    "duration" => [
                        "gt" => isset($params["min"]) ? $params["min"] : 0,
                        "lte" => isset($params["max"]) ? $params["max"] : 10000000
                    ]
                ]
            ];
            $mustRule[] = $range;
        }
        return $mustRule;
    }

    /**
     * mark videos as deleted
     * @param $tube string
     * @param $ids array
     * @return bool
     */
    public function setDeleted($tube, $ids)
    {
        return $this->updateMany(["deleted" => true], $tube, $ids);
    }


    public function addOne($data)
    {
        $query = [
            'index' => $this->name,
            'type' => '_doc',
            'body' => $data,
        ];
        if (isset($data['tube']) && isset($data['external_id'])) {
            //generate unified id, otherwise document will be indexed to autogenerated_id
            $id = $this->generateId($data['tube'], $data['external_id']);
            $query["id"] = $id;
        }
        return $this->add($query);
    }

    public function addMany($data)
    {
        if (!count($data)) return false;
        $finalD = [];
        foreach ($data as $d) {
            $meta = [
                "index" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                ]
            ];

            if (isset($d['tube']) && isset($d['external_id'])) {
                $id = $this->generateId($d['tube'], $d['external_id']);
                $meta["index"]["_id"] = $id;
            }
            array_push($finalD, $meta, $d);
        }
        return $this->bulkAdd($finalD);
    }

    public function getOne($tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $this->generateId($tube, $external_id)
        ];
        return $this->get($params);
    }


    public function updateOne($data, $tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $this->generateId($tube, $external_id),
            'retry_on_conflict' => 3,
            'body' => [
                'doc' => $data
            ]
        ];
        return $this->update($params);
    }

    public function updateMany($data, $tube, $external_ids)
    {
        if (!count($external_ids)) return false;
        $params = [];
        foreach ($external_ids as $id) {
            $params[] = [
                "update" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                    "_id" => $this->generateId($tube, $id)
                ]
            ];
            $params[] = [
                "doc" => $data
            ];
        }
        return $this->bulkUpdate($params);
    }

    public function deleteOne($tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $this->generateId($tube, $external_id)
        ];
        return $this->delete($params);
    }

    public function deleteMany($tube, $external_ids)
    {
        if (!count($external_ids)) return false;
        $params = [];
        foreach ($external_ids as $id) {
            $params[] = [
                "delete" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                    "_id" => $this->generateId($tube, $id)
                ]];
        }
        return $this->bulkDelete($params);
    }

    /**
     * get last stored video
     * @param $tube
     * @return array|null
     */
    public function getLastStored($tube)
    {
        $params = [
            'index' => $this->name,
            'size' => 1,
            'body' => [
                'query' => [
                    "bool" => [
                        "filter" => ["term" => ["tube" => $tube]]
                    ]
                ],
                "sort" => ["external_id" => ["order" => "desc"]]
            ],
        ];
        $res = $this->search($params);
        if ($res) {
            return isset($res["data"]) && !empty($res["data"]) ? $res["data"][0] : null;
        }
        return null;
    }

    protected function generateId($tube, $external_id)
    {
        return "$tube-$external_id";
    }

}