#!/usr/bin/env perl

use strict;
use FindBin ();
use lib "$FindBin::Bin/lib";
use Test::More;
use Test::RabbitMQ::Config;
use RabbitMQ::Constants qw( AMQP_PRECONDITION_FAILED );

use_ok('RabbitMQ');

my $mq     = RabbitMQ->new;
my $sockfd = $mq->connect(
    {
        host     => HOST,
        port     => PORT,
        user     => USER,
        password => PASSWORD,
        vhost    => VHOST,
    }
);

my $channel = 5532;
my $ch = eval { $mq->channel_open($channel) };
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );

my $queue = 'basic_cancel';
# Test for declaring a queue
{
    local $@;
    my $declared_queue = eval {
        $ch->queue_declare(
            $queue,
            {
                passive     => 0,
                durable     => 0,
                auto_delete => 0,
                exclusive   => 0,
            }
        );
    };
    is( $declared_queue->{queue},
        $queue, "Declared a queue " . $declared_queue->{queue} );
}

# Test for cancelling a consume
{
    local $@;
    my $consumer_tag = eval { $ch->basic_consume( $queue, {} ) };
    ok( $consumer_tag, "Start a consumer $consumer_tag" );

    my $cancelled_consumer_tag =
      eval { $ch->basic_cancel( $consumer_tag, {} ) };
    is( $cancelled_consumer_tag, $consumer_tag,
        "Cancelled a consumer $cancelled_consumer_tag" );
}

# Test for cancelling a consume with an empty consumer-tag
{
    local $@;
    my $consumer_tag           = eval { $ch->basic_consume( $queue, {} ) };
    my $cancelled_consumer_tag = eval { $ch->basic_cancel( "",      {} ) };
    my $expected_error_message =
      "precondition-failed " . AMQP_PRECONDITION_FAILED;
    like( $@, qr/$expected_error_message/, "Cannelled with an empty consumer-tag" );
}

# Test for deleting a queue
{
    local $@;
    my $deleted_queue = $ch->queue_delete( $queue, {} );
    is( $deleted_queue->{queue},
        $queue, "Deleted a queue " . $deleted_queue->{queue} );
}

is( $mq->channel_close($channel), 1, "Closed a channel $channel" );
is( $mq->disconnect,              0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
