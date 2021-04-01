<?php
namespace Roxby\Elastic;

class Response
{
    public static function success($data = null) :array
    {
        $response = ["success" => true];
        if(!is_null($data)) $response["result"] = $data;
        return $response;
    }

    public static function error(\Exception $exception) :array
    {
        return [
            "success" => true,
            "error" => $exception->getMessage()
        ];
    }
}