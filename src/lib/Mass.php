<?php
namespace Leo\lib;
use Aws\S3\S3Client;

class Mass extends Uploader{
	private $bucket;
	private $tempFile;
	private $fhandle;
	private $client;

	private $id;

	public $combine = true;
	// public $batch_size = 1024 * 1024 * 100;
	public $batch_size = 104857600;
	// public $record_size = 1024 * 1024 * 5;
	public $record_size = 5242880;
	public $max_records = 20;
	// public $duration = 60 * 60* 60;
	public $duration = 216000;

	private $opts;
	private $uploader;

	public function __construct($id, $bucket, $region, $uploader, $opts=[]) {
		ini_set('memory_limit', '500M');
		$this->id = $id;

		$this->opts = array_merge([
			'tmpdir'=>isset($opts['tmpdir'])?$opts['tmpdir']:sys_get_temp_dir()
		], $opts);

		$this->bucket = $bucket;
		$this->uploader = $uploader;

		$this->client = new S3Client([
			"version"=>"2006-03-01",
			"region"=>$region,
			 'http'    => [
		        'verify' => false
		    ]
		]);
	}

    /**
     * Initializes the gz file handler.
     * This was in the __construct, but caused a new file to be created on every object instantiation.
     */
    public function initializeFileHandler()
    {
        $this->tempFile = tempnam($this->opts['tmpdir'], 'leo');
        $this->fhandle  = gzopen($this->tempFile, 'wb6');

    }

	public function sendRecords($batch) {

	    //Initialize the file handler. Was in construct, but that led to many orphan files
	    if (empty($this->tempFile)) {
	        $this->initializeFileHandler();
        }

		$correlation = "";
		foreach($batch['records'] as $record) {
			gzwrite($this->fhandle, $record['data']);
		}
		return [
				"success"=>true,
				"correlation"=>$correlation
			];
	}

	public function end() {
		gzclose($this->fhandle);

		$handler = fopen($this->tempFile,'r');

		$key = "bus_v2/{$this->id}/" . Utils::milliseconds() . ".gz";
		$result = $this->client->putObject([
			'Body'=> $handler,
			'Bucket'=> $this->bucket,
			'Key'=> $key
		]);

		/*
		* @todo  check if it was a success or not
		*/
		fclose($handler);
		unlink($this->tempFile);

		var_dump($this->tempFile);


		$this->uploader->end();
		return;
	}
}