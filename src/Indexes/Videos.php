<?php

namespace Roxby\Elastic\Indexes;

class Videos extends AbstractIndex
{
    public $name = 'videos';

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
     * @param $query string - search query
     * @param $tube string ["analdin', 'xozilla', 'vintagetube']
     * @param $params
     * - from integer
     * - size integer
     * - fields assoc array of search fields [field1 => 1, field2 => 3, field3 => 10]
     * @return array
     */

    public function searchMany($query, $tube, array $params = [])
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
            $fieldsArr = [
                "title^3",
                "title.english^3",
                "cats^10",
                "cats.english^10",
                "tags",
                "tags.english",
                "models",
                "models.english"
            ];
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
                    "filter" => [
                        "term" => ["tube" => $tube]
                    ],
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
}