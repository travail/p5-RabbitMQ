#!/usr/bin/env perl

use strict;
use FindBin ();
use lib "$FindBin::Bin/lib";
use Test::More;
use Test::RabbitMQ::Config;

use_ok('RabbitMQ');

$SIG{'PIPE'} = "IGNORE";

my $mq       = RabbitMQ->new;
my $sockfd = $mq->connect(
    {
        host        => HOST,
        port        => PORT,
        user        => USER,
        password    => PASSWORD,
        vhost       => VHOST,
        channel_max => 0,
    }
);
my $channel = 5532;
my $ch      = $mq->channel_open($channel);

# Test for opening channel
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );
is( $mq->channel_close($channel), 1, "Closed channel $channel" );

# Test for opening a channel which have been already opened
{
    local $@;
    eval { $mq->channel_open($channel) };
    is( $@, '', "Opened a channel $channel" );
    eval { $mq->channel_open($channel) };
    isnt( $@, '',
        "Could not open a channel $channel which already have been opend" );
}

# Test for opening a channel with 0
{
    local $@;
    $channel = 0;
    eval { $mq->channel_open($ch) };
    isnt( $@, '', "Could not open a channel $channel" );
}

# Test for opening a channel with string
{
    local $@;
    $channel = 'foo';
    eval { $mq->channel_open($channel) };
    isnt( $@, '', "Could not open a channel with string $channel" );
}

is( $mq->disconnect, 0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
