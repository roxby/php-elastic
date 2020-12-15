<?php

namespace Roxby\Elastic\Indexes;

class Videos extends AbstractIndex
{
    public $name = 'videos';
    protected static $instance = null;

    const SORT_BY_RELEVANCE = 'relevance';
    const SORT_BY_ID_ASC = 'id-asc';
    const SORT_BY_ID_DESC = 'id-desc';
    const SORT_BY_POST_DATE = 'post_date';
    const SORT_BY_RATING = 'rating';
    const SORT_MOST_VIEWED = 'video_viewed';
    const SORT_BY_DURATION = 'duration';
    const SORT_BY_COMMENTS = 'most_commented';
    const SORT_BY_FAVOURITES = 'most_favourited';


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
    public function getProps()
    {
        return [
            'external_id' => ['type' => 'long'],
            'title' => [
                'type' => 'text',
                'fields' => [
                    'keyword' => ['type' => 'keyword'],
                    'english' => ['type' => 'text', 'analyzer'=> 'english']
                ]
            ],
            'description' => [
                'type' => 'text',
                'fields' =>
                    ['english' => ['type' => 'text', 'analyzer' => 'english']]
            ],
            'duration' => ['type' => 'integer'],
            'rating' => ['type' => 'integer'],
            'rating_amount' => ['type' => 'integer'],
            'video_viewed' => ['type' => 'integer'],
            'post_date' => [
                'type' => 'date',
                'format' => "yyyy-MM-dd HH:mm:ss"
            ],
            'models' => [
                'type' => 'text',
                'fields' =>
                    ['english' => ['type' => 'text', 'analyzer' => 'english']]
            ],
            'cats' => [
                'type' => 'text',
                'fields' =>
                    ['english' => ['type' => 'text', 'analyzer' => 'english']]
            ],
            'tags' => [
                'type' => 'text',
                'fields' =>
                    ['english' => ['type' => 'text', 'analyzer' => 'english']]
            ],
            'tube' => ['type' => 'keyword'],
            'deleted' => ['type' => 'boolean'],
            'video_page' => ['type' => 'text'],
            'thumb' => ['type' => 'text'],
            'vthumb' => [
                'properties' => [
                    'link' => ['type' => 'text'],
                    'w' => ['type' => 'integer'],
                    'h' => ['type' => 'integer']]
            ],
            'comments_count' => ['type' => 'integer'],
            'favourites_count' => ['type' => 'integer'],
            'is_hd' => ['type' => 'boolean']
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
     * @param $sort string
     * @return array
     */

    public function getMany($tube, $query, array $params, $sort)
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($defaults, $params);
        $mustRule = $this->buildMustRule($query, $params);

        $filter = [
            ["term" => ["tube" => $tube]],
        ];
        if (isset($params['is_hd'])) {$filter[] = ["term" => ["is_hd" => $params['is_hd']]];}

        $body = [
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => [
                "bool" => [
                    "must" => $mustRule,
                    "filter" => $filter,
                    "must_not" => ["match" => ["deleted" => true]],
                ]
            ]
        ];
        if ($sort !== self::SORT_BY_RELEVANCE) {
            $body["sort"] = $this->sort($sort);
        }
        $data = [
            'index' => $this->name,
            'body' => $body
        ];
        return $this->search($data);
    }



    /**
     * build must rule to add to search query
     * Post date not greater than today
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
        $mustRule = [
            [
                "multi_match" => [
                    "query" => $query,
                    "fields" => [
                        "models^11",
                        "models.english^11",
                        "title^10",
                        "title.english^10",
                        "description^9",
                        "description.english^9",
                        "tags^8",
                        "tags.english^8",
                        "cats^7",
                        "cats.english^7"
                    ],
                    "minimum_should_match" => "75%"
                ]
            ]
        ];


        if ($params && (isset($params['min']) || isset($params['max']))) {
            $range = [
                "range" => ["duration" => ["gt" => $params["min"] ?? 0, "lte" => $params["max"] ?? 10000000]]
            ];
            $mustRule[] = $range;
        }

        return $mustRule;
    }


    /**
     * add sort to search query
     * @param $sort
     * @return array
     */
    private function sort($sort)
    {
        switch ($sort) {
            case self::SORT_BY_ID_DESC:
                return ["external_id" => ["order" => "desc"]];
            case self::SORT_BY_ID_ASC:
                return ["external_id" => ["order" => "asc"]];
            case self::SORT_BY_DURATION:
                return ["duration" => ["order" => "desc"]];
            case self::SORT_MOST_VIEWED:
                return ["video_viewed" => ["order" => "desc"]];
            case self::SORT_BY_RATING:
                return [
                    "_script" => [
                        "type" => "number",
                        "script" => [
                            "source" => "doc.rating.value / doc.rating_amount.value;",
                        ],
                        "order" => "desc"
                    ]
                ];
            case self::SORT_BY_COMMENTS:
                return ["comments_count" => ["order" => "desc"]];
            case self::SORT_BY_FAVOURITES:
                return ["favourites_count" => ["order" => "desc"]];
            case self::SORT_BY_POST_DATE:
            default:
                return ["post_date.keyword" => ["order" => "desc"]];
        }
    }

    /**
     * mark videos as deleted
     * @param $tube string
     * @param $ids array
     * @return integer
     */
    public function setDeleted($tube, $ids)
    {
        return $this->updateMany(["deleted" => true], $tube, $ids);
    }

    /**
     * insert one videos
     * @param $data
     * @return bool
     */
    public function addOne($data)
    {
        $query = [
            'index' => $this->name,
            'body' => $data,
        ];
        if (isset($data['tube']) && isset($data['external_id'])) {
            //generate unified id, otherwise document will be indexed to autogenerated_id
            $id = $this->generateId($data['tube'], $data['external_id']);
            $query["id"] = $id;
        }
        return $this->add($query);
    }

    /**
     * bulk insert
     * @param $data
     * @return integer
     */
    public function addMany($data)
    {
        if (!count($data)) return false;
        $res = [];
        foreach ($data as $d) {
            if (!isset($d['tube']) || !isset($d['external_id'])) continue;

            $meta = [
                "index" => [
                    "_index" => $this->name,
                    "_id" => $this->generateId($d['tube'], $d['external_id'])
                ]
            ];

            array_push($res, $meta, $d);
        }
        return $this->bulkAdd($res);
    }

    /**
     * get one document by id
     * @param $tube
     * @param $external_id
     * @return array|null
     */
    public function getById($tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $external_id)
        ];
        return $this->get($params);
    }


    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/updating_documents.html#_partial_document_update
     * update one document
     * @param $data
     * @param $tube
     * @param $external_id
     * @return bool
     */
    public function updateOne($data, $tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $external_id),
            'retry_on_conflict' => 3,
            'body' => ['doc' => $data]
        ];
        return $this->update($params);
    }

    public function updateMany($data, $tube, $external_ids)
    {
        if (!count($external_ids)) return false;
        $params = [];
        foreach ($external_ids as $id) {
            array_push($params,
                ["update" => ["_index" => $this->name, "_id" => $this->generateId($tube, $id)]],
                ["doc" => $data]
            );
        }
        return $this->bulkUpdate($params);
    }

    /**
     * delete one document
     * @param $tube
     * @param $external_id
     * @return bool
     */
    public function deleteOne($tube, $external_id)
    {
        $params = [
            'index' => $this->name,
            'id' => $this->generateId($tube, $external_id)
        ];
        return $this->delete($params);
    }

    /**
     * bulk delete
     * @param $tube
     * @param $external_ids
     * @return bool
     */
    public function deleteMany($tube, $external_ids)
    {
        if (!count($external_ids)) return false;
        $params = [];
        foreach ($external_ids as $id) {
            $params[] = [
                "delete" => [
                    "_index" => $this->name,
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
                    "bool" => ["filter" => ["term" => ["tube" => $tube]]]
                ],
                "sort" => ["post_date.keyword" => ["order" => "desc"]]
            ],
        ];
        $res = $this->search($params);
        return $res["data"][0] ?? null;
    }

    protected function generateId($tube, $external_id)
    {
        $id =  "$tube-$external_id";
        return md5($id);
    }

}



