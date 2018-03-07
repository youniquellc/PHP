<?php
require __DIR__ . '/SdkTestCase.php';

/**
 * Test Stand alone cron monitoring
 * Class CronTest
 */
final class CronTest extends SdkTestCase
{
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

    /**
     * Test checkpoint bot
     * @throws Exception
     */
    public function testError()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $this->assertTrue($cron->end(new Exception('Bad code')));
    }
}
