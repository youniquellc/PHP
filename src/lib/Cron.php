<?php

class Cron {
	private $id;
	private $runId;

	private $message;
	private $runAgain;

	public function __contstruct($id,$runId=null) {
		$this->id = $id;
	}


	public function aquireLock() {
		//CHeck lock    cron.checkLock

		//flag as started

	}

	public function releaseLock() {
		//removes lock


		//flag as completed  cron.reportComplete

		//store message, runAgain Flag
	}


	public function checkpoint() {

	}


}
?>