<?php

namespace Leo\lib;

use Aws\DynamoDb\DynamoDbClient;
use Aws\S3\S3Client;

/**
 * Class Events
 * @package Leo\lib
 */
class Events
{
    /**
     * @var DynamoDbClient
     */
    private $dynamoDBClient;

    /**
     * @var S3Client
     */
    private $s3Client;

    /**
     * @var
     */
    private $config;

    /**
     * Events constructor.
     * @param $config
     */
    public function __construct($config)
    {
        $this->config = $config;
        $this->dynamoDBClient = new DynamoDbClient([
            "region" => $config['region'],
            "credentials" => $config['credentials'],
            "version" => "2012-08-10",
            'http' => [
                'verify' => false
            ]
        ]);
        $this->s3Client = new S3Client([
            "region" => $config['region'],
            "credentials" => $config['credentials'],
            "version" => "2006-03-01",
            'http' => [
                'verify' => false
            ]
        ]);
    }

    /**
     * Get Event Range
     * @param $id
     * @param $queue
     * @param array $opts
     * @return array|null
     * @throws \Exception
     */
    public function getEventRange($id, $queue, $opts = [])
    {
        $dynamoDB = new DynamoDB($this->dynamoDBClient, null, $this->config);
        $range = $dynamoDB->getEventRange($id, $queue, $opts);

        return $range;
    }

    /**
     * Get Event Reader
     * @param $id
     * @param $queue
     * @param $range
     * @param $opts
     * @return DynamoDBReader|DynamoDBReaderV2|EventIterator
     * @throws \Exception
     */
    public function getEventReader($id, $queue, $range, $opts)
    {
        if (empty($range)) {
            throw new \Exception("Event \"$queue\" does not exist");
        }

        if (isset($range['s3']) && $range['s3']) {
            return (new S3($this->s3Client, $this->dynamoDBClient))->getReader($queue, $range, $opts);
        } else {
            $dynamoDB = new DynamoDb($this->dynamoDBClient, $this->s3Client, $this->config);
            return $dynamoDB->getReader($id, $queue, $range, $opts);
        }
    }
}
