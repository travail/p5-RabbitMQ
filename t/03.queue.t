#!/usr/bin/env perl

use strict;
use FindBin ();
use lib "$FindBin::Bin/lib";
use Test::More;
use Test::RabbitMQ::Config;

use_ok('RabbitMQ');

my $mq       = RabbitMQ->new;
my $sockfd   = $mq->connect(
    {
        host     => HOST,
        port     => PORT,
        user     => USER,
        password => PASSWORD,
        vhost    => VHOST,
    }
);

my $queue   = 'task_queue';
my $channel = 5532;
my $ch = eval { $mq->channel_open($channel) };
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );

# Test for declaring a queue
{
    my $declared_queue = eval {
        $ch->queue_declare( $queue,
            { passive => 0, durable => 0, exclusive => 0, auto_delete => 0 }
        );
    };
    is( $declared_queue->{queue},
        $queue, "Declared a queue as " . $declared_queue->{queue} );
}

# Test for declaring q queue which is aleady exists with settting 1 to passive
{
    local $@;
    my $declared_queue = eval {
        $ch->queue_declare( $queue,
            { passive => 1, durable => 1, exclusive => 0, auto_delete => 1 }
        );
    };
    is( $declared_queue->{queue}, $queue,
        "Declared an existing queue as " . $declared_queue->{queue} );
}

# Test for deleting a non queue
{
    local $@;
    my $deleted_queue = eval {
        $ch->queue_delete( $queue, { if_unused => 0, if_empty => 0 } );
    };
    is( $deleted_queue->{queue}, $queue, "Deleted a queue " . $deleted_queue->{queue} );
}

is( $mq->channel_close($channel), 1, "Closed channel $channel" );
is( $mq->disconnect, 0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
