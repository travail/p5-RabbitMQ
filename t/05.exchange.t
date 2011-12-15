use Test::More;
use strict;
use Data::Dumper;

use_ok('RabbitMQ');

my $host     = '192.168.1.1';
my $port     = 5672;
my $user     = 'guest';
my $password = 'guest';
my $vhost    = '/';
my $mq       = RabbitMQ->new;
my $sockfd   = $mq->connect(
    {
        host     => $host,
        port     => $port,
        user     => $user,
        password => $password,
        vhost    => $vhost,
    }
);

my $channel = 5532;
my $ch = eval { $mq->channel_open($channel) };
is( ref $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
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
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
