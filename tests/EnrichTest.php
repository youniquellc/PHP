<?php
use PHPUnit\Framework\TestCase;

final class EnrichTest extends TestCase
{
    public function testLoad() {
    	$leo = new Leo\Sdk("MadeUpBot8", [
			"enableLogging"=>false,
			"firehose"=>"Leo-BusToS3-KECVSTB21J92",
			"kinesis"=>"Leo-KinesisStream-ATNV3XQO0YHV",
			"s3"=>"leo-s3bus-1r0aubze8imm5",
			"debug"=>true
		]);
		$leo->createEnrichment("gti.test", function ($event,$meta) {
			// \Leo\lib\Utils::log($event);

			return ['newEvent'=>"stuve", 'oldEvent'=>$event];
		}, "gti.enriched",[
			"start"=>"z/2017/08/05/20/45/1501987540151-0029990"
		]);
    }
}