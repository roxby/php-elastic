<?php

namespace App\Indexes;

/**
 * Class AbstractIndex
 * @package App\Indexes
 *
 *
 * indexExists
 * indexCreate
 *
 * add / bulkAdd
 * search
 * update
 * delete

 *
 */

abstract class AbstractIndex
{

    public $client;
    public $name;
    public $props_mapping;

    public function __construct()
    {
        $config = include('../../config.php');
        $this->client = \Elasticsearch\ClientBuilder::create()->setHosts($config['hosts'])->build();
    }


    public function exist()
    {
        return $this->client->indices()->exists(['index' => $this->name]);
    }

    public function create()
    {
        $params = [
            'index' => $this->name,
            'body' => [
                'settings' => [
                    'analysis' => [
                        'analyzer' => [
                            "roxby_analyzer" => [
                                "tokenizer" => "standard",
                                //for plural forms
                                //uses porter algorithm to cut word endings and suffixes
                                "filter" => ["standard", "lowercase", "porter_stem"]]]
                    ]],
                'mappings' => [
                    '_doc' => [
                        '_source' => [
                            'enabled' => true
                        ],
                        'properties' => $this->props_mapping
                    ]
                ]
            ]
        ];
        $response = $this->client->indices()->create($params);
        return isset($response['acknowledged']) ? $response['acknowledged'] : false;
    }


    /**
     * Is this add single document into index?
     * Lets try to have more than default IDE generated comments for these libraries
     *
     * @see there is `see` tag where you can put API documentation url here and there
     * @param $data
     * @param $params
     * @return bool
     */
    public function add($data, $id = null)
    {
        try {
            $defaults = [
                "type" => "_doc",
                "index" => $this->name
            ];
            $params = array_merge($params, $defaults);

            $query = [
                'index' => $params['index'], // is this index name?
                'type' => $params['type'], // hardcoded _doc
                'body' => $data,
            ];
            if ($id) {
                $query['id'] = $id;
            }
            if (isset($data['id'])) {
                $query['id'] = $data['id'];
            }
            $response = $this->client->index($query);
            return !$response["errors"];
        } catch (\Exception $exception) {
            // Read about errors handling in php packages (best practices)
            // i am not sure that you are handling errors on package level
            error_log($exception->getMessage());
            return false;
        }
    }


    /**
     * @param $data
     * @param array $params
     * @return array|bool
     */
    public function bulkStore($data, $params = [])
    {
        try {
            $defaults = [
                "type" => "_doc",
                "index" => $this->name
            ];
            $params = array_merge($params, $defaults);
            $finalD = [];
            foreach ($data as $d) {
                $meta = [
                    "index" => [
                        "_index" => $params['index'],
                        "_type" => $params['type'],
                        "_id" => $d['id']
                    ]
                ];

                array_push($finalD, $meta, $d);
            }
            $responses = $this->client->bulk(['body' => $finalD]);

            return isset($responses['errors']) ? !$responses['errors'] : false;
        } catch (\Exception $exception) {
            error_log($exception->getMessage());
            return false;
        }

    }

    public function update($id, $data, $params)
    {
        $defaults = [
            "type" => "_doc",
            "index" => $this->name
        ];
        $params = array_merge($params, $defaults);
        $params = [
            'index' => $params['index'],
            'type' => $params['type'],
            'id' => $data['id'],
            'body' => [
                'doc' => $data
            ]
        ];
        return $this->client->update($params);
    }

    public function delete($id, $params = [])
    {
        $defaults = [
            "type" => "_doc",
            "index" => $this->name
        ];
        $params = array_merge($params, $defaults);
        $params = [
            'index' => $params['index'],
            'type' => $params['type'],
            'id' => $id
        ];
        return $this->client->delete($params);
    }

    public function search($params)
    {
        try {
            $data = [
                'index' => $this->name,
                'body' => $this->searchQuery($params)
            ];

            $results = $this->client->search($data);

            /**
             * @eugene
             * We need to align about PHP versions we are using on our environments. This way we might
             * use lang more effectively and be better engineers ЯХУХУЯХУ :)
             */
            if ($results["hits"] && $results["hits"]["hits"]) {
                return $results["hits"]["hits"];
            }
            return null;
        } catch (\Exception $exception) {
            error_log($exception->getMessage());
            return null;
        }

    }

    abstract public function searchQuery($params);

}

