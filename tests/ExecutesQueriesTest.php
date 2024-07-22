<?php

namespace Matchory\Elasticsearch\Tests;

use Elasticsearch\Client;
use Matchory\Elasticsearch\Collection;
use Matchory\Elasticsearch\Connection;
use Matchory\Elasticsearch\Query;
use Matchory\Elasticsearch\Tests\Traits\ESQueryTrait;
use PHPUnit\Framework\TestCase;
use Mockery;

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
                'body' => ['scroll_id' => 'abc123456789']
            ])
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

        $clientMock->expects($this->never())
            ->method('clearScroll');

        $collection = $this->getQueryObjectWithClient($clientMock)->clear();

        $this->assertInstanceOf(Collection::class, $collection);
        $this->assertEmpty($collection);
    }

    public function testScrollWithScrollId()
    {
        $clientMock = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $clientMock->expects($this->once())
            ->method('scroll')
            ->with([
                'body' => [
                    'scroll' => null,
                    'scroll_id' => 'abc123456789'
                ]
            ])
            ->willReturn([]);

        $response = $this->getQueryObjectWithClient($clientMock)
            ->performSearch('abc123456789');

        $this->assertIsArray($response);
        $this->assertEmpty($response);
    }

    public function testScrollWithScrollIdAndScrollSet()
    {
        $clientMock = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $clientMock->expects($this->once())
            ->method('scroll')
            ->with([
                'body' => [
                    'scroll' => '5m',
                    'scroll_id' => 'abc123456789'
                ]
            ])
            ->willReturn([]);

        $query = $this->getQueryObjectWithClient($clientMock);
        $query->scroll();

        $response = $query->performSearch('abc123456789');

        $this->assertIsArray($response);
        $this->assertEmpty($response);
    }

    public function testScrollWithoutScroll()
    {
        $clientMock = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $clientMock->expects($this->never())
            ->method('scroll');

        $clientMock->expects($this->once())
            ->method('search')
            ->willReturn([]);

        $query = $this->getQueryObjectWithClient($clientMock);
        $query->scroll();

        $response = $query->performSearch();

        $this->assertIsArray($response);
        $this->assertEmpty($response);
    }

    public function test_search_with_track_total_hits()
    {
        $clientMock = Mockery::mock(Client::class)->makePartial();

        $clientMock->shouldReceive('scroll')
            ->never();

        $clientMock->shouldReceive('search')
            ->once()
            ->withArgs(function(...$args) {
                $params = $args[0];
                $this->assertTrue($params['track_total_hits']);
                return true;
            })
            ->andReturn([]);

        $query = $this->getQueryObjectWithClient($clientMock);
        $query->trackTotalHits(true);

        $response = $query->performSearch();

        $this->assertIsArray($response);
        $this->assertEmpty($response);
    }

    public function test_search_with_track_total_hits_as_int()
    {
        $clientMock = Mockery::mock(Client::class)->makePartial();

        $clientMock->shouldReceive('scroll')
            ->never();

        $clientMock->shouldReceive('search')
            ->once()
            ->withArgs(function(...$args) {
                $params = $args[0];
                $this->assertEquals(50000, $params['track_total_hits']);
                return true;
            })
            ->andReturn([]);

        $query = $this->getQueryObjectWithClient($clientMock);
        $query->trackTotalHits(50000);

        $response = $query->performSearch();

        $this->assertIsArray($response);
        $this->assertEmpty($response);
    }

    public function test_search_without_track_total_hits()
    {
        $clientMock = Mockery::mock(Client::class)->makePartial();

        $clientMock->shouldReceive('scroll')
            ->never();

        $clientMock->shouldReceive('search')
            ->once()
            ->withArgs(function(...$args) {
                $params = $args[0];
                $this->assertArrayNotHasKey('track_total_hits', $params);
                return true;
            })
            ->andReturn([]);

        $query = $this->getQueryObjectWithClient($clientMock);

        $response = $query->performSearch();

        $this->assertIsArray($response);
        $this->assertEmpty($response);
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
