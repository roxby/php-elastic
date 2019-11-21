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
     * @param $sourceLanguage
     * @param string $targetLanguage
     * @return array
     */
    public function translate($query, $sourceLanguage, $targetLanguage = 'en')
    {
        try {
            $result = $this->translator->translate($query, [
                'source' => $sourceLanguage,
                'target' => $targetLanguage
            ]);
            return [
                'success' => isset($result['text']),
                'text' => $result['text'] ?? null
            ];
        } catch (\Exception $exception) {
            return [
                'success' => false,
                'error' => $exception->getMessage()
            ];
        }

    }
}