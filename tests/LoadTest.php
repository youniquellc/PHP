<?php
require __DIR__ . '/SdkTestCase.php';

/**
 * Test Loading
 * Class LoadTest
 */
final class LoadTest extends SdkTestCase
{
    /**
     * Test Load
     * @throws Exception
     */
    public function testLoad()
    {
        $stream = $this->sdk->createLoader(function ($something) {
            $this->assertNotEmpty($something);
            $this->assertTrue($something['success']);
        });

        for ($i = 0; $i < 100000; $i++) {
            $stream->write($this->test_queue, [
                'id' => "test-$i",
                'data' => 'sample data',
                'other data' => 'other data'
            ], [
                'source' => null,
                'start' => $i
            ]);
        }

        $stream->end();
    }
}
