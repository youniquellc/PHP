LeoPlatform/PHP
===================

LEO PHP SDK

A php interface to interact with the LEO Platform

Documentation: https://docs.leoplatform.io

How to install the LEO SDK
===================================

Pre-Requisites
--------------
1. Install the aws-cli toolkit - Instructions for this are found at http://docs.aws.amazon.com/cli/latest/userguide/installing.html
2. Configure the aws-cli tools - Instructions are found at http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html


Install SDK
-----------
1. Two ways to install.  
 
Directly from composer:  (https://getcomposer.org/doc/01-basic-usage.md)

```
curl -sS https://getcomposer.org/installer | php
php composer.phar require leoplatform/php
```

Or using the GitHub Repository:


Create or add to your composer.json

```
{
    "repositories": [
        {
            "type": "vcs",
            "url": "https://github.com/LeoPlatform/PHP.git"
        }
    ],
    "require": {
        "leoplatform/php": "dev-master"
    }
}
```

Then run the install command:  

```
$ curl -sS https://getcomposer.org/installer | php
$ php composer.phar install
```

Or if you already have composer installed:

```
$ composer install
```

Example Usage
-------------

Load events to LEO Platform

```
<?php

require_once("vendor/autoload.php");

//Create the leo object with configuration to your AWS resources.
//These values are required.  See docs for how to obtain them
$config = [
	"enableLogging"	=> false,
	"debug"	=> false,
	"kinesis"  => "Leo-KinesisStream-?????",
	"firehose" => "Leo-BusToS3-?????",
	"s3"     => "leo-s3bus-?????"		
];

//create a Leo-sdk object
$leo = new Leo\Sdk("BotName", $config);

//These are optional parameters, see the docs for possible values
$stream_options = [];

//function is called with every commit to the stream
//returns data about the checkpoint
$checkpoint_function = function ($checkpointData) {
	var_dump($checkpointData);
};

////create a loader stream
$stream = $leo->createLoader($checkpoint_function, $stream_options);

for($i = 0; $i < 100000; $i++) {
	$event = [
		"id"=>"testing-$i",
		"data"=>"some order data",
		"other data"=>"some more data"
	];
	$meta = ["source"=>null, "start"=>$i];
  
	//write an event to the stream
	$stream->write("QueueName", $event, $meta);
}
$stream->end();
```

Enrich Events on LEO 

```
<?php 
require_once("vendor/autoload.php");

$config = [
	"enableLogging"	=> false,
	"debug"	=> false,
	"kinesis"	=> "Leo-KinesisStream-BKKSUCR98KK9",
	"firehose"	=> "Leo-BusToS3-KECVSTB21J92",
	"s3"		=> "leo-s3bus-ifrthoxhkqcy"
];

$bot_name = "EnrichmentBot";
$in_queue_name = "QueueName";
$enriched_queue_name = "EnrichedQueueName";
$read_options = ['limit'=>100000, 'run_time'=> "4 minutes"];

$transform_function = function ($event, $meta) {
			var_dump($event);
			$event["newdata"] = "this is some new data";
			return [
				"newEvent"=>$event
			];
		};

$leo = new Leo\Sdk($bot_name, $config);

$stream = $leo->createEnrichment($in_queue_name, $transform_function, $enriched_queue_name, $read_options );
```

Offload Events From LEO
-----------------------

```
<?php 
require_once("vendor/autoload.php");

use LEO\Stream;

$config = [
	"enableLogging"	=> false,
	"debug"	=> false,
	"kinesis"	=> "Leo-KinesisStream-BKKSUCR98KK9",
	"firehose"	=> "Leo-BusToS3-KECVSTB21J92",
	"s3"		=> "leo-s3bus-ifrthoxhkqcy"
];

$bot_name = "OffloaderBot";
$queue_name = "EnrichedQueueName";
$target_name = "TargetSystem";
$read_options = ['limit'=>50, 'run_time'=> "4 minutes"];

$offload_function = function ($event) {
			var_dump($event);
			
			//Do some API calling to offload the data
			
			return [
				"result"=>$result
			];
		};

$leo = new Leo\Sdk($bot_name, $config);

$reader = $leo->createOffloader($queue_name, $read_options);
		foreach($reader->events as $i=>$event) {
			\Leo\lib\Utils::log($event);	
			$count++;
			if(++$checkpointCount == 1000) {
				$reader->checkpoint();
				$checkpointCount=0;
			}
		}
		$reader->checkpoint();
		\Leo\lib\Utils::log($count);

```


Logging
-------
The LEO SDK will pass your PHP logs up to the LEO Platform so that you can debug them using the Data Innovation Center user interface.

You do this when you instantiate a new Leo\Sdk object by passing in **enableLogging** with a value of **true**.

```
$test = new Stream("Test Bot", [
	"enableLogging"=>true,
	"config"=>[
		"firehose" => "Leo-BusToS3-??????????",
		"kinesis"  => "Leo-LeoStream-??????????",
		"mass"	   => "leo-s3bus-??????????"
	]
]);

```

Autoload
---------------------

The LEO PHP SDK uses Composer's autoload functionality to include classes that are needed.  Including the following code into any php file in your project will automatically load the LEO SDK.

```
require_once("vendor/autoload.php");
```
