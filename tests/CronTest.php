<?php
require_once __DIR__ . '/../vendor/autoload.php';
use PHPUnit\Framework\TestCase;
use Aws\Credentials\CredentialProvider;
use Aws\Sts\StsClient;
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
    private $role_arn = '';
    private $lambda_function = '';

    /**
     * Setup
     */
    public function setUp()
    {
        $cred_provider = NULL;
        $role_provider = CredentialProvider::assumeRole([
            'client' => new StsClient([
                'region' => 'us-west-2',
                'version' => 'latest',
            ]),
            'assume_role_params' => [
                'RoleArn' => $this->role_arn,
                'RoleSessionName' => 'test_session',
            ],
        ]);
        $cred_provider = CredentialProvider::memoize($role_provider);
        $this->sdk = new Sdk('TestPHPCronBot', [
            'credentials' => $cred_provider,
            'enableLogging' => false,
            'cron_lambda_function' => $this->lambda_function,
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
        $cron->start();
        $this->assertTrue($cron->end());
    }

    /**
     * Test checkpoint bot
     * @throws Exception
     */
    public function testCheckpoint()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $cron->start();
        $this->assertTrue($cron->checkpoint(500));
    }

    /**
     * Test checkpoint bot
     * @throws Exception
     */
    public function testError()
    {
        $cron = $this->sdk->createCron($this->test_queue);
        $cron->start();
        $this->assertTrue($cron->end(new Exception('Bad code')));
    }
}
