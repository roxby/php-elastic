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
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html
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
                        '_source' => [
                            'enabled' => true
                        ],
                        'properties' => $this->getProps()
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
     * return index mapping
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html
     * @return array|null
     */
    public function getMapping()
    {
       try {
           $res = $this->client->indices()->getMapping(['index' => $this->name]);
           return $res[$this->name]['mappings']['properties'] ?? [];
       } catch (\Exception $exception) {
           return null;
       }
    }

    /**
     * Get document by its id - uid in our case
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/getting_documents.html
     * @param $params
     * @return array|null
     */
    protected function get($params)
    {
        try {
            $res = $this->client->get($params);
            return $res["found"] ? $res["_source"] : null;
        } catch (\Exception $exception) {
            return null;
        }

    }

    /**
     * Add single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html#_single_document_indexing
     * @param $query
     * @return bool
     */
    protected function add($query)
    {
        try {
            $this->client->index($query);
            return true;
        } catch (\Exception $exception) {
            return false;
        }
    }


    /**
     * bulk indexing of documents
     * bulk API expects JSON action/metadata pairs. We're pushing meta data and object itself for each inserted document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html#_bulk_indexing
     * @param $data
     * @return bool
     */
    protected function bulkAdd($data)
    {
        $responses = $this->client->bulk(['body' => $data]);
        return isset($responses['errors']) ? !$responses['errors'] : false;
    }



    /**
     * Update single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/updating_documents.html
     * @param $params
     * @return bool
     */
    protected function update($params)
    {
        try {
            $this->client->update($params);
            return true;
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return false;
        }

    }

    /**
     * @param $params
     * @return integer
     */
    protected function bulkUpdate($params)
    {
        $responses = $this->client->bulk(["body" => $params]);
        $updated = 0;
        if(isset($responses['errors']) && isset($responses['items'])) {
            $gotErrors = $responses['errors'];
            $items = $responses['items'];
            if(!$gotErrors) {
                return count($items);
            }

            foreach ($items as $item) {
                $result = $item['update']['result'] ?? null;
                if($result == 'updated' || $result == 'noop') {
                    $updated++;
                }
            }
        }
        return $updated;
    }

    /**
     * Delete single document by id
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/deleting_documents.html
     * @param $params
     * @return bool
     */
    protected function delete($params)
    {
        try {
            $this->client->delete($params);
            return true;
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return false;
        }
    }

    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     * @param $params
     * @return bool
     */
    protected function bulkDelete($params)
    {
        $responses = $this->client->bulk(["body" => $params]);
        return isset($responses['errors']) ? !$responses['errors'] : false;
    }


    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/search_operations.html
     * @param array $params
     * @return array|null
     */
    protected function search($params)
    {
        try {
            $results = $this->client->search($params);
            if ($results["hits"]) {
                $results = $results["hits"];
                $sources = array_map(function ($res) {
                    return $res["_source"];
                }, $results["hits"]);

                $total = $results["total"]["value"] ?? 0;
                return [
                    "data" => $sources,
                    "total" => $total
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
     * Get count of documents
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
     * @param $query
     * @return integer
     */
    public function count($query)
    {
        $params = [
            'index' => $this->name,
            'body' => [
                "query" => $query
            ]
        ];
        $res = $this->client->count($params);
        return isset($res["count"]) ? $res["count"] : 0;
    }

    /**
     * @return array of index properties
     */
    abstract function getProps();


}

