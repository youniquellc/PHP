<?php
namespace Leo\lib;

abstract class Uploader {
	public $batch_size;
	public $record_size;
	public $max_records;
	public $duration;

	public $combine;
}