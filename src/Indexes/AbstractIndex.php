<?php

namespace Roxby\Elastic\Indexes;


abstract class AbstractIndex
{

    public $client;
    public $name;

    public function __construct($hosts = null)
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
        $response = $this->client->indices()->create($params);
        return isset($response['acknowledged']) ? $response['acknowledged'] : false;
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
     *      * Add single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_indexing_documents.html - single doc indexing
     * @param $data
     * @param null $id
     * @return array
     */
    public function add($data, $id = null)
    {
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
        return $this->client->index($query);


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
                    "_type" => "_doc"
                ]
            ];
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
     * @return array
     */
    public function update($data, $id)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $id,
            'body' => [
                'doc' => $data
            ]
        ];
        return $this->client->update($params);
    }

    /**
     * Delete single document by id
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_deleting_documents.html
     * @param $id
     * @return array
     */
    public function delete($id)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'id' => $id
        ];
        return $this->client->delete($params);
    }

    /**
     * Delete single document by query
     * @param $query array
     * @return array
     */
    public function deleteByQuery($query)
    {
        $params = [
            'index' => $this->name,
            'type' => '_doc',
            'body' => [
                'query' => [
                    'match' => $query

                ]
            ]
        ];
        return $this->client->deleteByQuery($params);
    }

    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/_search_operations.html
     * @param $query string
     * @param $tube
     * @param array $params
     * @return array|null
     */
    public function search($query, $tube, array $params = [])
    {

        $params = [
            'index' => $this->name,
            'body' => $this->searchQuery($query, $tube, $params)
        ];

        $results = $this->client->search($params);
        if ($results["hits"] && $results["hits"]["hits"]) {
            return $results["hits"]["hits"];
        }
        return null;


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
     * @return array of index properties
     */
    abstract function getIndexMapping();

    /**
     * build search query, index specific
     * @param $query string
     * @param $tube
     * @param $params
     * @return array
     */
    abstract public function searchQuery($query, $tube, array $params);

}

