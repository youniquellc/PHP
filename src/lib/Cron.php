<?php
namespace Leo\lib;
use Aws\Lambda\LambdaClient;

/**
 * Class Cron
 */
class Cron
{
    /**
     * @var null Bot ID
     */
    private $id = null;

    /**
     * @var null Bot Start Token
     */
    private $token = null;

    /**
     * @var null Bot eid
     */
    private $eid = null;

    /**
     * @var null Bot Output Queue
     */
    private $queue = null;

    /**
     * @var LambdaClient|null
     */
    private $client = null;

    /**
     * @var null Lambda Function name
     */
    private $lambda_function_name = null;

    /**
     * Cron constructor.
     * @param $id
     * @param $queue
     * @param $region
     * @param $lambda_function_name
     */
    public function __construct($id, $queue, $region, $lambda_function_name)
    {
        $this->id = $id;
        $this->queue = $queue;
        $this->lambda_function_name = $lambda_function_name;
        $this->client = new LambdaClient([
            "region"=> $region,
            "version"=>"2015-03-31",
            'http'    => [
                'verify' => false
            ]
        ]);
    }

    /**
     * Report cron start
     * @return bool
     */
    public function start()
    {
        $params = [
            'id' => $this->id,
            'type' => 'start'
        ];

        $result = $this->client->invoke([
                'FunctionName' => $this->lambda_function_name,
                'InvocationType' => 'RequestResponse',
                'Payload' => json_encode($params)
        ]);

        $this->token = $result['token'];

        return $result['status'] === 'success';
    }

    /**
     * Report cron end
     * @param string $status
     * @return bool
     */
    public function end($status='complete')
    {
        $params = [
            'id' => $this->id,
            'type' => 'end',
            'status' => $status,
            'token' => $this->token
        ];

        $result = $this->client->invoke([
            'FunctionName' => $this->lambda_function_name,
            'InvocationType' => 'RequestResponse',
            'Payload' => json_encode($params)
        ]);

        return $result['status'] === 'success';
    }

    /**
     * Report cron end with checkpoint units
     * @param int $units
     * @param string $type
     * @return bool
     */
    public function checkpoint($units=1, $type='write')
    {
        $params = [
            'id' => $this->id,
            'type' => 'end',
            'token' => $this->token,
            'checkpoint' => [
                'eid' => $this->eid,
                'units' => $units,
                'type' => $type
            ]
        ];

        $result = $this->client->invoke([
            'FunctionName' => $this->lambda_function_name,
            'InvocationType' => 'RequestResponse',
            'Payload' => json_encode($params)
        ]);

        return $result['status'] === 'success';
    }

}
