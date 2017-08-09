<?php

 namespace Leo\lib;
 class Utils {
	public static function milliseconds() {
   		return round(microtime(true) * 1000);
 	}

 	public static function log($stuff) {
 		fwrite(STDOUT, var_export($stuff, true) . "\n");
 	}
 }