<?php
require __DIR__ . '/SdkTestCase.php';

/**
 * Test Enrich
 * Class EnrichTest
 */
final class EnrichTest extends SdkTestCase
{
    /**
     * Test enrich
     * @throws Exception
     */
    public function testEnrich()
    {
        $this->sdk->createEnrichment($this->test_queue, function ($event, $meta) {
            $this->assertNotNull($event);
            $this->assertNotEmpty($meta);
            return ['newEvent' => "enriched test", 'oldEvent' => $event];
        }, "$this->test_queue-enriched", [
            "limit" => 10
        ]);
    }
}
