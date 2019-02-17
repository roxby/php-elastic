<?php

namespace Roxby\Elastic\Indexes;


abstract class AbstractIndex
{

    public $client;
    public $name;

    public function __construct($hosts)
    {
        $this->client = \Elasticsearch\ClientBuilder::create()->setHosts($hosts)->build();
    }


    /**
     * Check if index exist
     * @return bool
     */
    public function indexExists()
    {
        return $this->client->indices()->exists(['index' => $this->name]);
    }

    /**
     * create new index
     * @return bool
     */
    public function indexCreate()
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
                            'properties' => $this->getIndexMapping()
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

    public function indexGetMapping()
    {
        if ($this->indexExists()) {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
            ];
            $res = $this->client->indices()->getMapping($params);
            return isset($res[$this->name]['mappings']['_doc']['properties']) ? $res[$this->name]['mappings']['_doc']['properties'] : [];
        }
        return [];

    }

    /**
     * Get document by its id
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
     * @param null $id
     * @return bool
     */
    public function add($data, $id = null)
    {
        try {
            $query = [
                'index' => $this->name,
                'type' => '_doc',
                'body' => $data,
            ];
            //if id is not null document will be indexed to index/type/id
            //else document will be indexed to index/type/autogenerated_id
            if ($id) {
                $query['id'] = $id;
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

            if (isset($d["uid"])) {
                $meta["index"]["_id"] = $d["uid"];
                unset($d["uid"]);
            }
            array_push($finalD, $meta, $d);
        }
        $responses = $this->client->bulk(['body' => $finalD]);
        return isset($responses['errors']) ? !$responses['errors'] : false;


    }

    /**
     * Update single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_updating_documents.html
     * @param $data
     * @param $id
     * @return bool
     */
    public function update($data, $id)
    {
        try {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
                'id' => $id,
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
     * Delete single document by id
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_deleting_documents.html
     * @param $id
     * @return bool
     */
    public function delete($id)
    {
        try {
            $params = [
                'index' => $this->name,
                'type' => '_doc',
                'id' => $id
            ];

            $this->client->delete($params);
            return true;
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return false;
        }

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
            if ($results["hits"] && $results["hits"]["hits"]) {
                $results = $results["hits"]["hits"];
                $sources = array_map(function ($res) {
                    return $res["_source"];
                }, $results);
                return $sources;
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
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
     * @param $tube
     * @return integer
     */
    public function count($tube)
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
    abstract function getIndexMapping();


}

