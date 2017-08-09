<?php
namespace LEO;

class Stream {
	private $id;
	private $opts;
	private $config;

	public function __construct($id=null, $opts=[]) {
		$this->id = $id;
		$this->opts = array_merge([
			"enableLogging"=>false,
			"version"=>"latest",
			"server"=>gethostname(),
			"uploader"=>"firehose"
		],$opts);

		$this->config = array_merge([
			"region"=>"us-west-2",
			"firehose"=>null,
			"kinesis"=>null,
			"s3"=>null
		], $opts['config']);
	}
	public function createBufferedWriteStream($opts=[], $checkpointer) {
		if(!$this->id) {
<<<<<<< HEAD
			throw new \Exeption("You must specify a bot id");
=======
			throw new \Exception("You must specify a bot id");
>>>>>>> release
		}

		switch($this->opts['uploader']) {
			case "firehose":
				$uploader = new Firehose($this->id, $this->config['firehose'], "us-west-2");
				break;
			case "kinesis":
				$uploader = new Kinesis($this->id, $this->config['kinesis'],"us-west-2");
				break;
			case "mass":
				$uploader = new Mass($this->id, $this->config['s3'],"us-west-2");
				break;
			case "ugradeable":
				$uploader = new Upgradeable($this->id, $this->config['firehose'], "us-west-2");
				break;

		}
		return new Combiner($this->id, $opts, $uploader,$checkpointer);
	}

	public function checkpoint() {
		
	}
	public function createTransformStream($queue, $toQueue, $opts, $transformFunc) {
		
	}
}