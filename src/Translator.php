<?php

namespace Roxby\Elastic;

use Google\Cloud\Translate\V2\TranslateClient;

class Translator
{
    private $translator;

    /**
     * Translator constructor.
     * @param $apiKey string
     */
    public function __construct($apiKey)
    {
        $this->translator = new TranslateClient(['key' => $apiKey]);
    }


    /**
     * @param $query
     * @param string $targetLanguage
     * @return array
     */
    public function translate($query, $targetLanguage = 'en')
    {
        try {
            $result = $this->translator->translate($query, [
                'target' => $targetLanguage
            ]);
            if (isset($result['text']) && isset($result['source'])) {
                return [
                    'success' => true,
                    'text' => $result['text'],
                    'source' => $result['source']
                ];
            } else {
                return ['success' => false];
            }
        } catch (\Exception $exception) {
            return [
                'success' => false,
                'error' => $exception->getMessage()
            ];
        }

    }
}