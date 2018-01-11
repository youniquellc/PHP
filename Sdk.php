<?php
/**
 * Leo Innovation Platform API Connection
 * Requires aws sdk version 3 which is installed into
 * a separate directory called aws3.
 */
namespace Leo;

/**
 * Class Sdk
 * @package Leo
 */
class Sdk
{
    private $config;
    private $id;

    /**
     * Sdk constructor.
     * @param $id
     * @param $opts
     * @throws \Exception
     */
    public function __construct($id, $opts)
    {
        $this->id = $id;
        $this->config = array_merge([
            "region" => "us-west-2",
            "firehose" => null,
            "kinesis" => null,
            "s3" => null,
            "enableLogging" => false,
            "uploader" => "kinesis",
            "version" => "latest",
            "server" => gethostname(),
            "debug" => true,
            "credentials" => null,
            "cron_lambda_function" => null
        ], $opts);
        if ($this->config['enableLogging']) {
            $this->enableLogging();
        }
    }

    /**
     * Loader
     * @param $checkpointer
     * @param array $opts
     * @return lib\Combiner
     * @throws \Exception
     */
    public function createLoader($checkpointer, $opts = [])
    {
        if (empty($opts['config'])) {
            $opts['config'] = [];
        }
        $opts['config'] = array_merge($opts['config'], $this->config);
        if (!$this->id) {
            throw new \Exception("You must specify a bot id");
        }

        switch ($opts['config']['uploader']) {
            case "firehose":
                $uploader = new lib\Firehose($this->id, $this->config['firehose'], $this->config['region']);
                $massuploader = new lib\Mass($this->id, $this->config['s3'], $this->config['region'], $uploader);
                break;
            case "kinesis":
                $uploader = new lib\Kinesis($this->id, $this->config['kinesis'], $this->config['region']);
                $massuploader = new lib\Mass($this->id, $this->config['s3'], $this->config['region'], $uploader);
                break;
            case "mass":
                $kinesis = new lib\Kinesis($this->id, $this->config['kinesis'], $this->config['region']);
                $uploader = new lib\Mass($this->id, $this->config['s3'], $this->config['region'], $kinesis);
                $massuploader = null;
                break;
            default:
                $uploader = null;
                $massuploader = null;
        }
        return new lib\Combiner($this->id, $opts, $uploader, $massuploader, $checkpointer);
    }

    /**
     * Offloader
     * @param $queue
     * @param array $opts
     * @return lib\DynamoDBReader|lib\DynamoDBReaderV2|\LEO\lib\EventIterator
     * @throws \Exception
     */
    public function createOffloader($queue, $opts = [])
    {
        $opts = array_merge([
            "buffer" => 1000,
            "loops" => 100,
            "debug" => false,
            "run_time" => new \DateInterval('P4M')
        ], $opts);

        if ($opts['run_time'] instanceof \DateInterval) {
            $opts['end_time'] = (new \DateTime())->add($opts['run_time']);
        } else if (!empty($opts['run_time'])) {
            $opts['end_time'] = new \DateTime("+ " . $opts['run_time']);
        }

        $events = new lib\Events($this->config);
        return $events->getEventReader($this->id, $queue, $events->getEventRange($this->id, $queue, $opts), $opts);
    }

    /**
     * Enrichment
     * @param $queue
     * @param $transform
     * @param $toQueue
     * @param array $opts
     * @throws \Exception
     */
    public function createEnrichment($queue, $transform, $toQueue, $opts = [])
    {
        $reader = $this->createOffloader($queue, $opts);
        $stream = $this->createLoader(function ($checkpoint) use ($reader) {
            lib\Utils::log($checkpoint);
            $reader->checkpoint($checkpoint);
        });
        $lastEvent = null;
        foreach ($reader->events as $i => $event) {
            $newEvent = $transform($event['payload'], $event);
            if ($newEvent) {
                $stream->write($toQueue, $newEvent, ["source" => $queue, "start" => $event['eid']]);
            }
        }

        $stream->end();
    }

    /**
     * Logging
     * @return lib\Logger
     * @throws \Exception
     */
    public function enableLogging()
    {
        if (!$this->id) {
            throw new \Exception("You must specify a bot id");
        }
        return new lib\Logger($this->id, $this->config);
    }

    /**
     * Cron
     * @param $queue
     * @return lib\Cron
     * @throws \Exception
     */
    public function createCron($queue)
    {
        if (!$this->config['cron_lambda_function']) {
            throw new \Exception('You must specify a cron lambda function');
        }

        return new lib\Cron(
            $this->id, $queue,
            $this->config['region'],
            $this->config['cron_lambda_function'],
            $this->config['credentials']
        );
    }
}
