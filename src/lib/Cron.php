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
     * @param $opts
     */
    public function __construct($id, $queue, $opts)
    {
        $this->id = $id;
        $this->queue = $queue;
        $this->lambda_function_name = $opts['cron_lambda_function'];
        $this->client = new LambdaClient([
            "credentials" => $opts['credentials'],
            "region"=> $opts['region'],
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
        $result = json_decode($result['Payload'], true);

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
            'status' => stripcslashes((string)$status),
            'token' => $this->token
        ];

        $result = $this->client->invoke([
            'FunctionName' => $this->lambda_function_name,
            'InvocationType' => 'RequestResponse',
            'Payload' => json_encode($params)
        ]);
        $result = json_decode($result['Payload'], true);

        return $result['status'] === 'success';
    }

    /**
     * Report cron end with checkpoint units
     * @param int $units
     * @param $eid
     * @param string $type
     * @return bool
     */
    public function checkpoint($units=1, $eid=null, $type='write')
    {
        if (!$eid) {
            $eid = strtotime('now');
        }

        $params = [
            'id' => $this->id,
            'type' => 'end',
            'token' => $this->token,
            'checkpoint' => [
                'eid' => $eid,
                'units' => $units,
                'type' => $type,
                'queue' => $this->queue
            ]
        ];

        $result = $this->client->invoke([
            'FunctionName' => $this->lambda_function_name,
            'InvocationType' => 'RequestResponse',
            'Payload' => json_encode($params)
        ]);
        $result = json_decode($result['Payload'], true);

        return $result['status'] === 'success';
    }

}
