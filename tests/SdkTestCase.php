<?php
require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Sdk.php';
require __DIR__ . '/../src/lib/Cron.php';
use PHPUnit\Framework\TestCase;
use Aws\Credentials\CredentialProvider;
use Aws\Sts\StsClient;
use Leo\Sdk;

class SdkTestCase extends TestCase {
    /**
     * @var Leo\Sdk | null
     */
    protected $sdk = null;

    /**
     * Test queue
     * @var string
     */
    protected $test_queue = 'TestLoadBotOut';

    /**
     * Test bot id
     * @var string
     */
    protected $test_id = 'TestLoadBot';

    /**
     * Role to assume
     * @var string
     */
    protected $role_arn = '';

    /**
     * Lambda function to invoke
     * @var string
     */
    protected $lambda_function = '';

    /**
     * Test region
     * @var string
     */
    protected $test_region = 'us-west-2';

    protected $test_firehose = '';
    protected $test_kinesis = '';
    protected $test_s3 = '';
    protected $test_cron_table = '';
    protected $test_leo_stream_table = 'L';
    protected $test_leo_event_table = '';

    /**
     * SdkTestCase constructor.
     * @param null $name
     * @param array $data
     * @param string $dataName
     */
    public function __construct($name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);

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
        $this->sdk = new Sdk($this->test_id, [
            'credentials' => $cred_provider,
            'enableLogging' => false,
            'firehose' => $this->test_firehose,
            'kinesis' => $this->test_kinesis,
            'leo_stream' => $this->test_leo_stream_table,
            'leo_cron' => $this->test_cron_table,
            'leo_event' => $this->test_leo_event_table,
            's3' => $this->test_s3,
            'cron_lambda_function' => $this->lambda_function,
            'region' => $this->test_region,
            'debug' => true
        ]);
    }
}
