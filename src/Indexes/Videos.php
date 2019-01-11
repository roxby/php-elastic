<?php
namespace App\Indexes;

class Videos extends AbstractIndex
{
    public $name = 'videos';

    public $props_mapping = [
        'video_id' => [
            'type' => 'integer',
        ],
        'title' => [
            'type' => 'text',
            'analyzer' => 'roxby_analyzer',
            'search_analyzer' => 'roxby_analyzer'
        ],
        'description' => [
            'type' => 'text',
            'analyzer' => 'roxby_analyzer',
            'search_analyzer' => 'roxby_analyzer'
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
            'analyzer' => 'roxby_analyzer',
            'search_analyzer' => 'roxby_analyzer'
        ],
        'cats' => [
            'type' => 'text',
            'analyzer' => 'roxby_analyzer',
            'search_analyzer' => 'roxby_analyzer'
        ],
        'tags' => [
            'type' => 'text',
            'analyzer' => 'roxby_analyzer',
            'search_analyzer' => 'roxby_analyzer'
        ],
        'tube' => [
            'type' => 'keyword'
        ]
    ];

    public function searchQuery($params)
    {
        $defaults = [
            "from" => 0,
            "size" => 100
        ];
        $params = array_merge($params, $defaults);
        return [
            "from" => $params['from'],
            "size" => $params['size'],
            "query" => [
                "bool" => [
                    "filter" => [
                        "term" => ["tube" => $params["tube"]]
                    ],
                    "should" => [
                        ["multi_match" => [
                            "query" => isset($params["query"]) ? $params["query"] : '',
                            "fields" => isset($params["fields"]) ? $params["fields"] : [],
                        ]]
                    ]
                ]
            ]
        ];
    }

}