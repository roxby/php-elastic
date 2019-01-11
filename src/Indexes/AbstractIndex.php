<?php

namespace App\Indexes;


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
     * @param $data
     * @param $params
     * @return bool
     */
    public function add($data, $params = [])
    {
        try {
            $defaults = [
                "type" => "_doc",
                "index" => $this->name
            ];
            $params = array_merge($params, $defaults);

            $res = [
                'index' => $params['index'],
                'type' => $params['type'],
                'body' => $data
            ];
            if (isset($data['id'])) {
                $res['id'] = $data['id'];
            }
            $response = $this->client->index($res);
            return !$response["errors"];
        } catch (\Exception $exception) {
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

    public function update($data, $params)
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

