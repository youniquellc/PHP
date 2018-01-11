<?php
use PHPUnit\Framework\TestCase;

final class ReadTest extends TestCase
{
    public function testLoad() {
    	$leo = new Leo\Sdk("GTITest", [
			"enableLogging"=>false,
			"firehose"=>"Leo-BusToS3-KECVSTB21J92",
			"kinesis"=>"Leo-KinesisStream-ATNV3XQO0YHV",
			"s3"=>"leo-s3bus-1r0aubze8imm5",
			"debug"=>true
		]);
		$reader = $leo->createOffloader("gti.test", [
			"start"=>"z/"
		]);

		$count = 0;
		$checkpointCount = 0;
		foreach($reader->events as $i=>$event) {
			// \Leo\lib\Utils::log($event);	
			$count++;
			if(++$checkpointCount == 1000) {
				$reader->checkpoint();
				$checkpointCount=0;
			}
		}
		$reader->checkpoint();
		\Leo\lib\Utils::log($count);
    }
}