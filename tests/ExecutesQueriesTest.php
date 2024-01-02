<?php

namespace Matchory\Elasticsearch\Tests;

use Elasticsearch\Client;
use Matchory\Elasticsearch\Collection;
use Matchory\Elasticsearch\Connection;
use Matchory\Elasticsearch\Query;
use Matchory\Elasticsearch\Tests\Traits\ESQueryTrait;
use PHPUnit\Framework\TestCase;

class ExecutesQueriesTest extends TestCase
{
    use ESQueryTrait;

    public function testClearWithScrollId()
    {
        $clientMock = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $clientMock->expects($this->once())
            ->method('clearScroll')
            ->with([
                'scroll_id' => 'abc123456789',
                'client' => [
                    'ignore' => []
                ]])
            ->willReturn([]);

        $collection = $this->getQueryObjectWithClient($clientMock)->clear('abc123456789');

        $this->assertInstanceOf(Collection::class, $collection);
        $this->assertEmpty($collection);
    }

    public function testClearWithoutScrollId()
    {
        $clientMock = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $clientMock->expects($this->once())
            ->method('clearScroll')
            ->with([
                'scroll_id' => null,
                'client' => [
                    'ignore' => []
                ]])
            ->willReturn([]);

        $collection = $this->getQueryObjectWithClient($clientMock)->clear();

        $this->assertInstanceOf(Collection::class, $collection);
        $this->assertEmpty($collection);
    }

    protected function getQueryObjectWithClient(Client $client): Query
    {
        return (new Query(
            new Connection(
                $client
            )
        ))
            ->index($this->index)
            ->type($this->type)
            ->take($this->take)
            ->skip($this->skip);
    }
}
