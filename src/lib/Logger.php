<?php
namespace Leo\lib;
use Aws\CloudWatchLogs\CloudWatchLogsClient;
use Aws\CloudWatchLogs\Exception\CloudWatchLogsException;
use Aws\Result;
use Exception;

/**
 * Class Logger
 * @package Leo\lib
 */
class Logger
{
    private $id;
    private $client;
    private $opts;

    private $messages = [];

    private $config;
    private $configFile;

    private $requestId;
    private $startTime;

    private $requestsPerSecond = 5; // CloudWatch limits are 5 requests per second
    private $messageLimit = 200000; // CloudWatch limits are 262144 per log event

    /**
     * Logger constructor.
     * @param $id
     * @param $opts
     */
    public function __construct($id, $opts)
    {
        $this->opts = $opts;
        $this->id = $id;
        $this->client = new CloudWatchLogsClient([
            "credentials" => $opts['credentials'],
            "region" => "us-west-2",
            "version" => "2014-03-28",
            'http' => [
                'verify' => false
            ]
        ]);

        $this->configFile = sys_get_temp_dir() . "/leo_log.json";
        $this->requestId = uniqid();
        $this->startTime = Utils::milliseconds();

        $this->getLogStream();

        register_shutdown_function([$this, "checkFatal"]);
        set_error_handler([$this, "logBuiltinError"]);
        set_exception_handler([$this, "logException"]);
        ini_set("display_errors", "on");
        ini_set('error_reporting', -1);

        $this->addMessage("START RequestId: {$this->requestId} Version: {$this->opts['version']}");
    }

    /**
     * Get log stream or create
     * @return array|mixed
     */
    private function getLogStream()
    {
        if ($this->config) {
            return $this->config;
        } else if (file_exists($this->configFile)) {
            //$config = json_decode(file_get_contents($this->configFile), JSON_OBJECT_AS_ARRAY);
            //check to see if the stream matches today's
            //if not, create a new one
            $logGroupName = "/aws/lambda/{$this->id}";
            try {
                $this->client->createLogGroup([
                    "logGroupName" => $logGroupName
                ]);
            } catch (CloudWatchLogsException $e) {
                //don't care about this one
            }
            $logStreamName = date("Y/m/d/") . "[{$this->opts['version']}]/{$this->opts['server']}/" . Utils::milliseconds();
            $this->client->createLogStream([
                "logGroupName" => $logGroupName,
                "logStreamName" => $logStreamName
            ]);
            $config = [
                "logGroupName" => $logGroupName,
                "logStreamName" => $logStreamName,
                "sequenceNumber" => null
            ];
        } else {
            $logGroupName = "/aws/lambda/{$this->id}";
            try {
                $this->client->createLogGroup([
                    "logGroupName" => $logGroupName
                ]);
            } catch (CloudWatchLogsException $e) {
                //don't care about this one
            }
            $logStreamName = date("Y/m/d/") . "[{$this->opts['version']}]/{$this->opts['server']}/" . Utils::milliseconds();
            $this->client->createLogStream([
                "logGroupName" => $logGroupName,
                "logStreamName" => $logStreamName
            ]);
            $config = [
                "logGroupName" => $logGroupName,
                "logStreamName" => $logStreamName,
                "sequenceNumber" => null
            ];
        }
        return $this->config = $config;
    }

    /**
     * Update config
     * @param $result
     */
    private function updateConfig(Result $result)
    {
        $this->config['sequenceNumber'] = $result->get("nextSequenceToken");
        file_put_contents($this->configFile, json_encode($this->config));
    }

    /**
     * Add message to messages to send
     * @param $message
     */
    private function addMessage($message)
    {
        $this->messages[] = [
            "timestamp" => Utils::milliseconds(),
            "message" => date('Y-m-d\TH:i:s\Z') . "	{$this->requestId}	" . $message
        ];

    }

    public function info()
    {

    }

    public function warn()
    {

    }

    public function error()
    {

    }

    public function debug($message, $file, $line)
    {

    }

    /**
     * Log exception
     * @param Exception|null$e
     */
    public function logException($e)
    {
        $this->log(get_class($e), $e->getMessage(), $e->getFile(), $e->getLine());
        $this->end();
        exit();
    }

    /**
     * Log built in error
     * @param $err
     * @param $message
     * @param $file
     * @param $line
     * @return bool
     */
    public function logBuiltinError($err, $message, $file, $line)
    {
        switch ($err) {
            case 1:
                $type = 'ERROR';
                break;
            case 2:
                $type = 'WARNING';
                break;
            case 4:
                $type = 'PARSE';
                break;
            case 8:
                $type = 'NOTICE';
                break;
            case 16:
                $type = 'CORE_ERROR';
                break;
            case 32:
                $type = 'CORE_WARNING';
                break;
            case 64:
                $type = 'COMPILE_ERROR';
                break;
            case 128:
                $type = 'COMPILE_WARNING';
                break;
            case 256:
                $type = 'USER_ERROR';
                break;
            case 512:
                $type = 'USER_WARNING';
                break;
            case 1024:
                $type = 'USER_NOTICE';
                break;
            case 2048:
                $type = 'STRICT';
                break;
            case 4096:
                $type = 'RECOVERABLE_ERROR';
                break;
            case 8192:
                $type = 'DEPRECATED';
                break;
            case 16384:
                $type = 'USER_DEPRECATED';
                break;
            case 30719:
                $type = 'ALL';
                break;
            default:
                $type = 'UNKNOWN';
                break;
        }

        return $this->log($type, $message, $file, $line);
    }

    /**
     * Log a message
     * @param $type
     * @param $message
     * @param $file
     * @param $line
     * @return bool
     */
    public function log($type, $message, $file, $line)
    {
        $this->addMessage(strtoupper($type) . ": " . $message . "\n\n $file $line");
        return false;
    }

    /**
     * Destruct call on class destruct
     */
    public function __destruct()
    {
        $this->end();
    }

    /**
     * Send events to cloud watch
     */
    private function sendEvents()
    {
        if (count($this->messages)) {

            list($messageChunkSize, $messageChunks) = $this->getMessageChunks();

            foreach ($messageChunks as $messageChunk) {
                $params = [
                    'logGroupName' => $this->config['logGroupName'],
                    'logStreamName' => $this->config['logStreamName'],
                    'logEvents' => $messageChunk
                ];
                if ($this->config['sequenceNumber']) {
                    $params['sequenceToken'] = $this->config['sequenceNumber'];
                }
                $result = $this->client->putLogEvents($params);
                $this->updateConfig($result);

                if ($messageChunkSize > $this->requestsPerSecond) {
                    usleep(250000); // send 4 requests per second
                }
            }

        }
        $this->messages = [];
    }

    /**
     * chunk $this->messages to avoid hitting CloudWatch limits
     *
     * @return array
     */
    private function getMessageChunks()
    {
        $messagesInBytes = mb_strlen(serialize((array)$this->messages), '8bit');
        $messagesByLimit = round(($messagesInBytes / $this->messageLimit), 0);
        $messageChunkSize = !$messagesByLimit ? 1 : $messagesByLimit;
        $messageChunks = array_chunk($this->messages, $messageChunkSize);
        return [$messageChunkSize, $messageChunks];
    }

    /**
     * End logging and send events
     */
    public function end()
    {
        if ($this->requestId) {
            $this->addMessage("END RequestId: {$this->requestId}");
            $duration = round(Utils::milliseconds() - $this->startTime, 2);
            $memory = round(memory_get_peak_usage() / 1024 / 1024);
            $memoryLimit = ini_get('memory_limit');
            $this->addMessage("REPORT RequestId: {$this->requestId}	Duration: $duration ms	Billed Duration: $duration ms Memory Size: $memoryLimit MB	Max Memory Used: $memory MB");


        }
        $this->sendEvents();
    }

    /**
     * Check fatal error
     */
    public function checkFatal()
    {
        $e = error_get_last();
        if ($e['type'] == E_ERROR) {
            $this->log($e['type'], $e['message'], $e['file'], $e['line']);
        }
    }
}
