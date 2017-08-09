<?php
namespace Leo\lib;
use Aws\DynamoDb\Marshaler;

class DynamoDB {
	/**
	* @var DynamoDBClient
	*/
	private $client;
	private $s3Client;
	private $marshaler;

	public function __construct($client,$s3Client=null) {
		$this->marshaler = new Marshaler();
		$this->client = $client;
		$this->s3Client = $s3Client;
	}

	public function getReader($id, $queue, $range,$opts) {
		if(empty($range['version']) || $range['version'] == 1) {
			return new DynamoDBReader($this->client, [
				"TableName"=>"Leo",
				"KeyConditionExpression"=>"#event = :event and #key between :start and :maxkey",
				"ExpressionAttributeNames"=> [
					"#event"=> "event",
					"#key"=> "kinesis_number",
				],
				'Limit'=>1000,
				"ExpressionAttributeValues"=>[
					":event"=>['S'=>$queue],
					":maxkey"=>['S'=>$range['max']]
				],
				"ReturnConsumedCapacity"=>'TOTAL'
			], $range['start'],$opts);
		} else {
			return new DynamoDBReaderV2($id, $this->client, $this->s3Client, [
				"TableName"=>"Leo_stream",
				"KeyConditionExpression"=>"#event = :event and #key between :start and :maxkey",
				"ExpressionAttributeNames"=> [
					"#event"=> "event",
					"#key"=> "end",
				],
				'Limit'=>10,
				"ExpressionAttributeValues"=>[
					":event"=>['S'=>$queue],
					":maxkey"=>['S'=>$range['max']],
					":start"=>['S'=>$range['start']]
				],
				"ReturnConsumedCapacity"=>'TOTAL'
			], $range, $opts);
		}
	}

	public function getEventRange($id, $event,$opts) {
		$result = $this->client->batchGetItem([
		    'RequestItems' => [ 
		        'Leo_cron' => [
		        	'Keys'=>[
		        		["id"=>['S'=>$id]]
		        	],
	            	'ConsistentRead' => true
		        ],
		        'Leo_event' => [
		        	'Keys'=>[
		        		["event"=> ['S'=>$event]]
		        	],
	            	'ConsistentRead' => true
		        ],
		    ],
		    'ReturnConsumedCapacity' => 'TOTAL',
		]);

		if($result['UnprocessedKeys'] && count($result['UnprocessedKeys'])) {
			throw new Exception("Not enough capacity to read");
		} else {
			$start = null;
			$leoEvent = null;
			$leoCron = null;

			$responses = $result->get('Responses');

			if (!$responses || empty($responses['Leo_event'])) { 
				return null;
			} else {
				$leoEvent = $this->marshaler->unmarshalItem($responses['Leo_event'][0]);
			}
			//Check if the cron bot doesn't exist yet
			if(empty($responses['Leo_cron'])) {
				$readObj = new \stdClass();
				$readObj->{'queue:'.$event} = [
					"checkpoint"=>null
				];
				$leoCron = [
					"id"=>$id,
					"checkpoints"=>[
						"read"=>$readObj,
						"write"=>new \stdClass()
					],
					"requested_kinesis"=>new \stdClass(),
					"lambda"=>new \stdClass(),
					"time"=>null,
					"instances"=>new \stdClass(),
					"lambdaName"=>null,
					"paused"=>false,
					"name"=>$id,
					"description"=>null
				];
				$result = $this->client->putItem([
					"TableName"=>"Leo_cron",
					"Item"=> $this->marshaler->marshalItem($leoCron)
				]);
				$compareStart = null;
			} else {
				$leoCron = $this->marshaler->unmarshalItem($responses['Leo_cron'][0]);
				if (!empty($leoCron['checkpoints']['read']['queue:'.$event])) {
					$compareStart = $leoCron['checkpoints']['read']['queue:'.$event]['checkpoint'];
				}
			}

			if (!empty($opts['start'])) {
				$start = $opts['start'];
			} else {
				if (!empty($compareStart)) {
					$start = $compareStart;
				} else {
					$start = "z/";
					$compareStart = null;
				}
			}
			if($start == null) {
				return null;
			}
			return [
				"start"=>$start, 
				"max"=>$leoEvent['max_eid'],
				"version"=>empty($leoEvent['v'])?1:$leoEvent['v'],
				"compare_start"=>$compareStart
			];	
		}
	}
}



class DynamoDBIteratorV1 implements \Iterator{
	private $client;
	private $marshaler;
	private $params;

	private $start;
	private $originalStart;
	private $position;

	private $cached;
	private $offset;

	private $last = null;
	private $opts;

	public function __construct($client, $params,$start,$opts) {
		$this->client = $client;
		$this->marshaler = new Marshaler();
		$this->params = $params;
		$this->start = $start;
		$this->originalStart = $start;
		$this->position = 0;
		$this->offset = 0;
		$this->cached = [];

		$this->opts = array_merge([
			"limit"=>1000000
		], $opts);

		$this->loadMore();
	}

	private function loadMore() {
		$this->offset += count($this->cached);
		if($this->position < $this->opts['limit'] &&  (new \DateTime()) < $this->opts['end_time']) {
			$this->params["ExpressionAttributeValues"][':start'] = ['S'=>$this->start. " "]; //we want it to start with the next one after start
			Utils::log($this->params);
			$result = $this->client->Query($this->params);
			$items = $result->get('Items');

			$this->cached = $items;
		} else {
			$this->cached = [];
		}

	}
	public function rewind() {
		if($this->start != $this->originalStart) {
			$this->start = $this->originalStart;	
			$this->position = 0;
			$this->offset = 0;
			$this->loadMore();
		} else {
			$this->position = 0;
		}
	}
	public function current() {
		$this->last = $this->marshaler->unmarshalItem($this->cached[$this->position-$this->offset]);
		$this->last['eid'] = $this->last['kinesis_number'];
		unset($this->last['kinesis_number']);


		return $this->last;
	}
	public function key() {
		return $this->position;
	}
	public function next() {
		++$this->position;
		if(count($this->cached) && ($this->position - $this->offset) >= count($this->cached)) {
			$this->start = $this->last['eid'];
			$this->loadMore();
		}
	}
	public function valid() {
		return isset($this->cached[$this->position-$this->offset]) && $this->position < $this->opts['limit'] && (new \DateTime()) < $this->opts['end_time'];
	}
};

class DynamoDBReader extends EventIterator {
	public $events;
	public function __construct($client, $params,$range,$opts) {
		$this->events = new DynamoDBIteratorV1($client, $params,$range,$opts);
	}
} 


class DynamoDBReaderV2 extends EventIterator {
	public $events;
	private $id;
	private $client;
	private $lastPosition = -1;
	private $queue;

	private $lastCheckpoint = null;

	public function __construct($id, $client, $s3Client, $params, $range, $opts) {
		$this->client = $client;
		$this->id = $id;
		$this->queue = $params['ExpressionAttributeValues'][':event']['S'];

		Utils::log($range);
		$this->lastCheckpoint = $range['compare_start'];

		$this->events = new DynamoDBIteratorV2($client, $s3Client, $params, $range['start'], $opts);
	}

	public function checkpoint($check=null){
		$checkpoint = null;
		$sourceTimestamp = null;
		$records = null;
		$marshaler = new Marshaler();

		if($check) {
			$checkpoint = $check['eid'];
			$records = $check['records'];
			$sourceTimestamp = null;
		} else {
			$event = $this->events->last;
			$records = ($event['position'] - $this->lastPosition);
			Utils::log($records);
			$this->lastPosition = $event['position'];

			$checkpoint = $event['eid'];
			$sourceTimestamp = $event['event_source_timestamp'];
		}

		Utils::log($records);
		Utils::log($sourceTimestamp);
		Utils::log($checkpoint);

		$result = $this->client->updateItem([
			'TableName'=>'Leo_cron',
			"Key"=>[
				"id"=>['S'=>$this->id]
			],
			"UpdateExpression"=>'set #checkpoints.#type.#event = :value',
			"ExpressionAttributeNames"=>[
				"#checkpoints"=>"checkpoints",
				"#type"=> "read",
				"#event"=> 'queue:'. $this->queue,
				"#checkpoint"=>"checkpoint"
			],
			"ConditionExpression"=>"attribute_not_exists(#checkpoints.#type.#event.#checkpoint) OR #checkpoints.#type.#event.#checkpoint = :expected",
			"ExpressionAttributeValues"=> $marshaler->marshalItem([
				":value"=> [
					"checkpoint"=> $checkpoint,
					"source_timestamp"=> $sourceTimestamp,
					"records"=>$records
				],
				":expected"=>$this->lastCheckpoint
			]),
		    'ReturnConsumedCapacity' => 'TOTAL',
		]);

		$this->lastCheckpoint = $checkpoint;
	}
}


class DynamoDBIteratorV2 implements \Iterator{
	private $client;
	private $s3Client;

	private $marshaler;
	private $params;

	private $start;
	private $originalStart;
	private $position;

	private $cached;
	private $offset;
	private $total = 0;

	public $last = null;
	private $opts;



	private $sources = [];

	public function __construct($client, $s3Client, $params,$start,$opts) {
		$this->client = $client;
		$this->s3Client = $s3Client;
		$this->marshaler = new Marshaler();
		$this->params = $params;
		$this->start = $start;
		$this->originalStart = $start;
		$this->position = 0;
		$this->offset = 0;
		$this->cached = [];

		$this->opts = array_merge([
			"limit"=>1000000
		], $opts);
		$this->loadMore();
	}

	private function gzipToRecords($eidTemplate, &$string) {
		list($prefix, $idOffset) = explode("-", $eidTemplate);
		$padLength = strlen($idOffset);
		$idOffset = intval($idOffset);
		$records = array_map(function($i) {
			return json_decode($i, true);
		}, explode("\n", gzdecode($string)));
		if($records[count($records)-1] == "") {
			array_pop($records);
		}
		foreach($records as &$record) {
			$record['eid'] = $prefix . "-" . str_pad($idOffset+$record['eid'], $padLength, "0", STR_PAD_LEFT);
			if($this->start < $record['eid']) {
				++$this->total;
				$this->cached[] = $record;
			}
		}
		Utils::log(count($records));
	}

	private function fromSources() {
		$this->offset += count($this->cached);
		$this->cached = [];
		$source =& $this->sources[0];

		if(!empty($source['s3'])) {
			$item = $this->marshaler->unmarshalItem($source);
			//find the right offset
			$skipped = 0;
			$chosenOffset = null;
			foreach($item['offsets'] as $i=>$offset) {
				list($prefix, $idOffset) = explode("-", $item['start']);
				$padLength = strlen($idOffset);
				$idOffset = intval($idOffset);
				$endEid =  $prefix . "-" . str_pad($idOffset+$offset['end'], $padLength, "0", STR_PAD_LEFT);
				if($this->start < $endEid) {
					$chosenOffset = $offset;
					if($i == count($item['offsets']) - 1) {
						array_shift($this->sources);
					}
					break;
				} else {
					$skipped = $offset['gzipOffset'] + $offset['gzipSize'];
				}
			}

			if(empty($source['file_location'])) { //gotta download this file
				$file_location =  tempnam(sys_get_temp_dir(), "Leo");
				$result = $this->s3Client->getObject([
				    'Bucket' => $item['s3']['bucket'],
				    'Key' => $item['s3']['key'],
				    'Range' => "bytes=" . ($skipped) . "-" . ($item['gzipSize'] - $chosenOffset['gzipOffset']),
				    'SaveAs'=> $file_location
				]);
				$item['file_location'] = $source['file_location']['S'] = $file_location;
			}

			list($prefix, $idOffset) = explode("-", $item['start']);
			$padLength = strlen($idOffset);
			$idOffset = intval($idOffset);
			$startEid =  $prefix . "-" . str_pad($idOffset+$chosenOffset['start'], $padLength, "0", STR_PAD_LEFT);
			$gzip = file_get_contents($item['file_location'], null, null, $skipped, $item['gzipSize'] - $skipped);
			$this->gzipToRecords($startEid, $gzip);
		} else if(!empty($source['gzip'])){
			$this->gzipToRecords($source['start']['S'], $source['gzip']['B']);
			array_shift($this->sources);
		}
	}

	private function loadMore() {
		if($this->position < $this->opts['limit'] &&  (new \DateTime()) < $this->opts['end_time'] &&
			$this->params["ExpressionAttributeValues"][':maxkey']['S'] > $this->start
			) {
			if(!empty($this->sources)) {
				$this->fromSources($this->start);
			} else {
				Utils::log($this->start);
				$this->params["ExpressionAttributeValues"][':start'] = ['S'=>$this->start. " "]; //we want it to start with the next one after start
				Utils::log($this->params);
				$result = $this->client->Query($this->params);
				$items = $result->get('Items');
				$this->sources = [];

				foreach($items as $i) {
					$this->sources[] = $i;
				}
				$this->fromSources($this->start);
			}
		} else {
			$this->cached = [];
		}

	}
	public function rewind() {
		if($this->start != $this->originalStart) {
			$this->start = $this->originalStart;	
			$this->position = 0;
			$this->offset = 0;
			$this->loadMore();
		} else {
			$this->position = 0;
		}
	}
	public function current() {
		$this->last = $this->cached[$this->position-$this->offset];
		$this->last['position'] = $this->position;
		return $this->last;
	}
	public function key() {
		return $this->position;
	}
	public function next() {
		++$this->position;
		if(count($this->cached) && ($this->position - $this->offset) >= count($this->cached)) {
			$this->start = $this->last['eid'];
			$this->loadMore();
		}
	}
	public function valid() {
		return isset($this->cached[$this->position-$this->offset]) && $this->position < $this->opts['limit'] && (new \DateTime()) < $this->opts['end_time'];
	}
};