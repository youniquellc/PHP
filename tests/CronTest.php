<?php
require_once __DIR__ . '/../vendor/autoload.php';
use PHPUnit\Framework\TestCase;
require_once __DIR__ . '/../Sdk.php';
require __DIR__ . '/../src/lib/Cron.php';
use Leo\Sdk;

final class CronTest extends TestCase
{
    /**
     * @var Leo\Sdk | null
     */
    private $sdk = null;

    /**
     * Test queue
     * @var string
     */
    private $test_queue = 'TestPHPCronBotOutput';

    /**
     * Setup
     */
    public function setUp()
    {
        $this->sdk = new Sdk('TestPHPCronBot', [
            'enableLogging' => false,
            'cron_lambda_function' => 'Leo-LeoBusApiProcessor-IY950D3J2MEE',
            'region' => 'us-west-2',
            'debug' => true
        ]);
    }

    /**
     * Test start bot
     * @throws Exception
     */
    public function testStart()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $this->assertTrue($cron->start());
    }

    /**
     * Test end bot
     * @throws Exception
     */
    public function testEnd()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $this->assertTrue($cron->end());
    }

    /**
     * Test checkpoint bot
     * @throws Exception
     */
    public function testCheckpoint()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $this->assertTrue($cron->checkpoint(500));
    }
}
