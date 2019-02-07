<?php

namespace Roxby\Elastic\Indexes;

class Videos extends AbstractIndex
{
    public $name = 'videos';

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

    /**
     * indexing text fields twice: once with the english analyzer and once with the standard analyzer.
     * @see https://qbox.io/blog/elasticsearch-english-analyzer-customize
     * @return array
     */
    public function getIndexMapping()
    {
        return [
            'video_id' => [
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
            ]
        ];
    }

    public function addEnglishAnalyzer()
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
     * build query filtered by tube name, should match search query, + possible add boost to certain fields
     * @param $tube string ["analdin', 'xozilla', 'vintagetube']
     * @param $query string - search query
     * @param $params
     * - from integer
     * - size integer
     * - fields assoc array of search fields [field1 => 1, field2 => 3, field3 => 10]
     * - min integer - minimum duration
     * - max integer - maximum duration
     * @return array
     */

    public function searchMany($tube, $query, array $params = [])
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($params, $defaults);

        if (isset($params['fields'])) {
            $fieldsArr = $this->buildSearchFields($params['fields']);
        } else {
            //defaults
            $fieldsArr = $this->fields;
        }
        $body = [
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => [
                "bool" => [
                    "must" => [
                        "multi_match" => [
                            "query" => $query,
                            "fields" => $fieldsArr
                        ]
                    ],
                    "filter" => $this->getFilter($tube, $params)
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
     * build query filter
     * - search for specific tube
     * - if duration params are set - search within duration range
     * @param $tube
     * @param $params
     * @return array
     */
    public function getFilter($tube, $params)
    {

        $filter = [];
        $filter[] = ["term" => ["tube" => $tube]];

        if (isset($params["min"]) || isset($params["max"])) {
            $duration = [];
            if (isset($params["min"])) {
                $duration["gt"] = $params["min"];
            }
            if (isset($params["max"])) {
                $duration["lte"] = $params["max"];
            }
            $range = [
                "bool" => [
                    "should" => [
                        "range" => ["duration" => $duration]
                    ]
                ]
            ];
            $filter[] = $range;

        }
        return $filter;
    }

    /**
     * Build array of search fields
     * Add additional english field (for english analyzer) to every text field for better
     * @param array $fields - expected structure [field1 => (int)boost, field2 => (int) boost, ...]
     * @return array
     */
    public function buildSearchFields(array $fields)
    {
        $result = [];
        static $mapping = null;
        if (is_null($mapping)) {
            $mapping = $this->indexGetMapping();
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
     * search one video filtered by tube + video id
     * @param $tube
     * @param $video_id
     * @return array|null
     */
    public function searchOne($tube, $video_id)
    {
        $body = ["query" => [
            "bool" => [
                "must" => [
                    ["term" => ["tube" => $tube]],
                    ["term" => ["video_id" => $video_id]]
                ]
            ]
        ]];
        $data = [
            "index" => $this->name,
            "body" => $body
        ];
        return $this->search($data);

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
                "sort" => ['video_id' => ['order' => 'desc']]
            ],
        ];
        $res = $this->search($params);
        if ($res) {
            return isset($res[0]['_source']) ? $res[0]['_source'] : null;
        }
        return null;
    }


    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
     * @param $tube
     * @param $query
     * @return integer
     */
    public function count($tube, $query)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'body' => [
                "query" => [
                    "bool" => [
                        "must" => [
                            "multi_match" => [
                                "query" => $query,
                                "fields" => $this->fields
                            ]

                        ],
                        "filter" => [
                            "term" => ["tube" => $tube]
                        ],
                    ]

                ]]];
        $res = $this->client->count($params);
        return isset($res["count"]) ? $res["count"] : 0;
    }

}