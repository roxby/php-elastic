<?php

namespace Roxby\Elastic\Indexes;


abstract class AbstractIndex
{

    protected $client;
    protected $name;

    protected function __construct($hosts)
    {
        $this->client = self::getClient($hosts);
    }

    protected static function getClient($hosts)
    {
        return \Elasticsearch\ClientBuilder::create()->setHosts($hosts)->build();
    }

    /**
     * Check if index exist
     * @param $hosts array
     * @param $name string
     * @return bool
     */
    public static function exists($hosts, $name)
    {
        try {
            $client = self::getClient($hosts);
            return $client->indices()->exists(['index' => $name]);
        } catch (\Exception $exception) {
            return $exception->getMessage();
        }

    }

    /**
     * create new index
     * @return bool
     */
    public function create()
    {
        try {
            $params = [
                'index' => $this->name,
                'body' => [
                    'mappings' => [
                        '_doc' => [
                            '_source' => [
                                'enabled' => true
                            ],
                            'properties' => $this->buildMapping()
                        ]
                    ]
                ]
            ];
            $this->client->indices()->create($params);
            return true;
        } catch (\Exception $exception) {
            return false;
        }

    }

    /**
     * @return array|null
     */
    public function getMapping()
    {
       try {
           $params = [
               'index' => $this->name,
               'type' => '_doc',
           ];
           $res = $this->client->indices()->getMapping($params);
           return isset($res[$this->name]['mappings']['_doc']['properties']) ? $res[$this->name]['mappings']['_doc']['properties'] : [];
       } catch (\Exception $exception) {
           return null;
       }
    }

    /**
     * Get document by its id - uid in our case
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/6.6/docs-get.html
     * @param $id
     * @return array|null
     */
    public function get($id)
    {
        try {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
                'id' => $id
            ];
            $res = $this->client->get($params);
            return $res["found"] ? $res["_source"] : null;
        } catch (\Exception $exception) {
            return null;
        }

    }

    /**
     * Add single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_indexing_documents.html - single doc indexing
     * @param $data
     * @return bool
     */
    public function add($data)
    {
        try {
            $query = [
                'index' => $this->name,
                'type' => '_doc',
                'body' => $data,
            ];
            //generate unified id, otherwise document will be indexed to autogenerated_id
            if (isset($data["tube"]) && isset($data["external_id"])) {
                $id = $this->generateUID($data["tube"], $data["external_id"]);
                $query["id"] = $id;
            }
            $this->client->index($query);
            return true;
        } catch (\Exception $exception) {
            return false;
        }
    }


    /**
     * bulk indexing of documents
     * bulk API expects JSON action/metadata pairs. We're pushing meta data and object itself for each inserted document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_indexing_documents.html - bulk indexing
     * @param $data
     * @return bool
     */
    public function bulkAdd($data)
    {
        $finalD = [];
        foreach ($data as $d) {
            $meta = [
                "index" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                ]
            ];

            if (isset($d["tube"]) && isset($d["external_id"])) {
                $id = $this->generateUID($d["tube"], $d["external_id"]);
                $meta["index"]["_id"] = $id;
            }
            array_push($finalD, $meta, $d);
        }
        $responses = $this->client->bulk(['body' => $finalD]);
        return isset($responses['errors']) ? !$responses['errors'] : false;
    }

    /**
     * @param $tube
     * @param $external_id
     * @return string
     */
    public function generateUID($tube, $external_id)
    {
        return "$tube-$external_id";
    }


    /**
     * Update single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_updating_documents.html
     * @param $data
     * @param $tube
     * @param $external_id
     * @return bool
     */
    public function update($data, $tube, $external_id)
    {
        try {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
                'id' => $this->generateUID($tube, $external_id),
                'body' => [
                    'doc' => $data
                ]
            ];
            $this->client->update($params);
            return true;
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return false;
        }

    }

    /**
     * @param $data
     * @param $tube
     * @param $external_ids
     * @return bool|string
     */
    public function bulkUpdate($data, $tube, $external_ids)
    {
        $params = [];
        foreach ($external_ids as $id) {
            $params[] =  [
                "update" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                    "_id" => $this->generateUID($tube, $id)
                ]
            ];
            $params[] = [
                "doc" => $data
            ];
        }
        $responses = $this->client->bulk(["body" => $params]);
        return isset($responses['errors']) ? !$responses['errors'] : false;
    }

    /**
     * Delete single document by id
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_deleting_documents.html
     * @param $tube
     * @param $external_id
     * @return bool
     */
    public function delete($tube, $external_id)
    {
        try {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
                'id' => $this->generateUID($tube, $external_id)
            ];

            $this->client->delete($params);
            return true;
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return false;
        }
    }

    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     * @param $tube
     * @param $external_ids
     * @return bool
     */
    public function bulkDelete($tube, $external_ids)
    {
        $params = [];
        foreach ($external_ids as $id) {
            $params[] = [
                "delete" => [
                    "_index" => $this->name,
                    "_type" => "_doc",
                    "_id" => $this->generateUID($tube, $id)
                ]];
        }
        $responses = $this->client->bulk(["body" => $params]);
        return isset($responses['errors']) ? !$responses['errors'] : false;
    }


    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_search_operations.html
     * @param array $params
     * @return array|null
     */
    public function search($params)
    {
        try {
            $results = $this->client->search($params);
            if ($results["hits"]) {
                $results = $results["hits"];
                $sources = array_map(function ($res) {
                    return $res["_source"];
                }, $results["hits"]);

                return [
                   "data" => $sources,
                    "total" => $results["total"]
                ];
            }
            return null;
        } catch (\Exception $exception) {
            return null;
        }

    }

    /**
     * explicitly refresh index, making all operations performed since the last refresh available for search
     * @see https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-refresh
     */
    public function indexRefresh()
    {
        $this->client->indices()->refresh(['index' => $this->name]);
    }

    /**
     * Get total count of documents related to specific tube
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
     * @param $tube
     * @return integer
     */
    public function countForTube($tube)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'body' => [
                "query" => ["term" => ["tube" => $tube]]
            ]
        ];
        $res = $this->client->count($params);
        return isset($res["count"]) ? $res["count"] : 0;
    }

    /**
     * @return array of index properties
     */
    abstract function buildMapping();


}

