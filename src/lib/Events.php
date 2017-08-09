<?php
namespace Leo\lib;
use Aws\DynamoDb\DynamoDbClient;
use Aws\S3\S3Client;


class Events {
	private $dynamoDBClient;

	public function __construct($config) {
		$this->dynamoDBClient = new DynamoDbClient(array(
			"region"=> $config['region'],
			"version"=>"2012-08-10",
			'http'    => [
				'verify' => false
			]
		));
		$this->s3Client = new S3Client(array(
			"region"=> $config['region'],
			"version"=>"2006-03-01",
			'http' => [
				'verify' => false
			]
		));
	}


	public function getEventRange($id, $queue,$opts=[]) {
		$dynamoDB = new DynamoDB($this->dynamoDBClient);
		$range = $dynamoDB->getEventRange($id, $queue,$opts);
		return $range;
	}

	public function getEventReader($id, $queue, $range,$opts) {
		if(empty($range)) {
			throw new \Exception("Event \"$queue\" does not exist");
		}
		if(isset($range['s3']) && $range['s3']) {
			return (new S3($this->s3Client, $this->dynamoDBClient))->getReader($queue, $range,$opts);
		} else {
			$dynamoDB = new DynamoDb($this->dynamoDBClient, $this->s3Client);
			return $dynamoDB->getReader($id, $queue, $range,$opts);
		}
	}
}
?>