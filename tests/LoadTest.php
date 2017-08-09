<?php
use PHPUnit\Framework\TestCase;

final class LoadTest extends TestCase
{
    public function testLoad() {
    	$leo = new Leo\Sdk("GTITest", [
			"enableLogging"=>false,
			"firehose"=>"Leo-BusToS3-KECVSTB21J92",
			"kinesis"=>"Leo-KinesisStream-ATNV3XQO0YHV",
			"s3"=>"leo-s3bus-1r0aubze8imm5",
			"debug"=>true
		]);
		$stream = $leo->createLoader(function ($something){
			// Leo\Lib\Utils::log($something);
		});
		for($i = 0; $i < 100000; $i++) {
			$stream->write("gti.test",[
				"id"=>"testing-$i",
				"data"=>"some order data",
				"other data"=>"some more data"
			], ["source"=>null, "start"=>$i]);
		}
		$stream->end();
    }
}