<?php

namespace Roxby\Elastic\Indexes;
use Roxby\Elastic\Response;

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
     * @return array
     */
    public static function exists(array $hosts, string $name) :array
    {
        try {
            $client = self::getClient($hosts);
            $exists =  $client->indices()->exists(['index' => $name]);
            return Response::success($exists);
        } catch (\Exception $exception) {
            return Response::error($exception);
        }

    }

    /**
     * create new index
     * @return array
     */
    public function create() :array
    {
        try {
            $params = [
                'index' => $this->name,
                'body' => $this->getSettings()
            ];
            $this->client->indices()->create($params);
            return Response::success();
        } catch (\Exception $exception) {
            return Response::error($exception);
        }
    }

    /**
     * set index mapping
     * @return array
     */
    public function setMapping() :array
    {
        try {
            $params = [
                'index' => $this->name,
                'body' => $this->getProps()
            ];
            $data = $this->client->indices()->putMapping($params);
            $res = $data["acknowledged"] ?? false;
            return Response::success($res);

        } catch (\Exception $exception) {
            return Response::error($exception);
        }
    }

    /**
     * return index mapping
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/index_management.html
     * @return array
     */
    public function getMapping() :array
    {
       try {
           $res = $this->client->indices()->getMapping(['index' => $this->name]);
           $props =  $res[$this->name]['mappings']['properties'] ?? [];
           return Response::success($props);

       } catch (\Exception $exception) {
           return Response::error($exception);
       }
    }

    /**
     * Get document by its id - uid in our case
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/getting_documents.html
     * @param $params
     * @return array
     */
    protected function get($params) :array
    {
        try {
            $res = $this->client->get($params);
            $doc =  $res["found"] ? $res["_source"] : [];
            return Response::success($doc);
        } catch (\Exception $exception) {
            return Response::error($exception);
        }

    }

    /**
     * Add single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html#_single_document_indexing
     * @param $query
     * @return array
     */
    protected function add($query) :array
    {
        try {
            $data = $this->client->index($query);

           $res = isset($data["result"]) && $data["result"] === "created";
           return Response::success(intval($res));
        } catch (\Exception $exception) {
            return Response::error($exception);
        }
    }


    /**
     * bulk indexing of documents
     * bulk API expects JSON action/metadata pairs. We're pushing meta data and object itself for each inserted document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/indexing_documents.html#_bulk_indexing
     * @param $data
     * @return array
     */
    protected function bulkAdd($data) :array
    {
        try {
            $responses = $this->client->bulk(['body' => $data]);
            $created = 0;
            if(isset($responses['items'])) {
                $items = $responses['items'];

                foreach ($items as $item) {
                    $result = $item['index']['result'] ?? null;
                    if($result == 'created') {
                        $created++;
                    }
                }
            }
            return Response::success($created);
        } catch (\Exception $exception) {
            return Response::error($exception);
        }

    }



    /**
     * Update single document
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/updating_documents.html
     * @param $params
     * @return array
     */
    protected function update($params) :array
    {
        try {
            $data = $this->client->update($params);
            $res = isset($data["result"]) && ($data["result"] == "updated" || $data["result"] == "created");
            return Response::success(intval($res));
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return Response::error($exception);
        }

    }

    /**
     * update many documents
     * @param $params
     * @return array
     */
    protected function bulkUpdate($params):array
    {
        try {
            $responses = $this->client->bulk(["body" => $params]);
            $updated = 0;
            if(isset($responses['errors']) && isset($responses['items'])) {
                $gotErrors = $responses['errors'];
                $items = $responses['items'];
                if(!$gotErrors) {
                    return Response::success(count($items));
                }

                foreach ($items as $item) {
                    $result = $item['update']['result'] ?? null;
                    if($result == 'updated' || $result == 'noop') {
                        $updated++;
                    }
                }
            }
            return Response::success($updated);
        } catch (\Exception $exception) {
           return Response::error($exception);
        }

    }

    /**
     * Delete single document by id
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/deleting_documents.html
     * @param $params
     * @return array
     */
    protected function delete($params) :array
    {
        try {
            $data = $this->client->delete($params);
            $res = isset($data["result"]) && $data["result"] == "deleted";
            return Response::success(intval($res));
        } catch (\Exception $exception) {
            //in case document not exists elastic returns not found exception, not false
            return Response::error($exception);
        }
    }

    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     * @param $params
     * @return array
     */
    protected function bulkDelete($params) :array
    {
        try {
            $data = $this->client->bulk(["body" => $params]);
            $items = $data["items"] ?? [];
            return Response::success(count($items));

        } catch (\Exception $exception) {
            Response::error($exception);
        }

    }


    /**
     * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/current/search_operations.html
     * @param array $params
     * @param bool $withIds
     * @return array
     */
    protected function search(array $params, bool $withIds = false) :array
    {
        try {
            $results = $this->client->search($params);
            if ($results["hits"]) {
                $results = $results["hits"];
                $sources = array_map(function ($res) use ($withIds){
                    $source = $res["_source"];
                    return $withIds ? array_merge(["id" => $res["_id"]], $source) : $source;
                }, $results["hits"]);

                $total = $results["total"]["value"] ?? 0;
                return Response::success([
                    "data" => $sources,
                    "total" => $total
                ]);
            }
            return Response::error(new \Exception("nothing found"));
        } catch (\Exception $exception) {
            return Response::error($exception);
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
     * @return array
     */
    public function count($query) :array
    {
        try {
            $params = [
                'index' => $this->name,
                'body' => [
                    "query" => $query
                ]
            ];
            $res = $this->client->count($params);
            $count = isset($res["count"]) ? $res["count"] : 0;
            return Response::success($count);
        } catch (\Exception $exception) {
            return Response::error($exception);
        }

    }

    /**
     * @return array of index fields mapping
     */
    abstract function getProps():array;

    /**
     * return index settings - analyzer, etc
     * @return array
     */
    abstract function getSettings(): array;


}

