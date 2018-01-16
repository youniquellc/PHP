<?php
require __DIR__ . '/SdkTestCase.php';

/**
 * Test reading
 * Class ReadTest
 */
final class ReadTest extends SdkTestCase
{
    /**
     * @var string
     */
    protected $test_id = 'TestReadBot';

    /**
     * Test Read
     * @throws Exception
     */
    public function testRead()
    {
        $reader = $this->sdk->createOffloader($this->test_queue, [
            "start" => "z/",
            "limit" => 10
        ]);

        $count = 0;
        $checkpointCount = 0;
        foreach ($reader->events as $i => $event) {
            $this->assertNotNull($event['timestamp']);
            $count++;
            if (++$checkpointCount == 10) {
                $reader->checkpoint();
                $checkpointCount = 0;
            }
        }

        $reader->checkpoint();
    }
}
