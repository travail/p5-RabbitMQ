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

my $channel = 5532;
my $ch = eval { $mq->channel_open($channel) };
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );

# Test for declaring an exchange
{
    local $@;
    my $exchange          = 'logs';
    my $declared_exchange = eval {
        $ch->exchange_declare( $exchange,
            { type => 'fanout', passive => 0, durable => 0 } );
    };
    is( $declared_exchange, $exchange,
        "Declared an exchange as $declared_exchange" );
}

# Test for re-declaring an exchange with the same parameters
{
    local $@;
    my $exchange          = 'logs';
    my $declared_exchange = eval {
        $ch->exchange_declare( $exchange,
            { type => 'fanout', passive => 0, durable => 0 } );
    };
    is( $declared_exchange, $exchange,
        "Re-declared an exchange $declared_exchange"
    );
}

# Test for re-reclaring an exchange with different parameters
{
    local $@;
    my $exchange = 'logs';
    my $declared_exchange = eval {
        $ch->exchange_declare( $exchange,
            { type => 'fanout', passive => 1, durable => 1 } );
    };
    is( $declared_exchange, $exchange,
        "Re-declared an exchnage $declared_exchange" );
}

# Test for deleting an exchange
{
    local $@;
    my $exchange = 'logs';
    my $deleted_exchange
        = eval { $ch->exchange_delete( $exchange, { if_unused => 1 } ) };
    is( $deleted_exchange, $exchange,
        "Deleted an exchange $deleted_exchange" );
}

# Test for deleting an undeclared exchange
{
    local $@;
    my $exchange = 'undeclared_exchagne';
    my $deleted_exchange
        = eval { $ch->exchange_delete( $exchange, { if_unused => 1 } ) };
    ok( $@, "Could not delete an undeclared exchange $exchange" );
}

# Test for deleting an exchange which is in use with setting 1 to if_unused
# TODO

# Test for deleting an exchange which is in use with setting 0 to if_unused
# TODO

is( $mq->channel_close($channel), 1, "Closed a channel $channel" );
is( $mq->disconnect, 0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
