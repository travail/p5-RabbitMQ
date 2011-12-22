use Test::More;
use strict;

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
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );

my $queue = 'basic_consume';
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

# Test for consuming
{
    local $@;
    my $consumer_tag
        = eval { $ch->basic_consume( $queue, {} ) };
    ok( $consumer_tag, "Got a consumer-tag $consumer_tag" );
}

# Test for consuming
{
    local $@;
    my $consumer_tag          = 'specified_consumer_tag';
    my $returned_consumer_tag = eval {
        $ch->basic_consume( $queue, { consumer_tag => $consumer_tag } );
    };
    is( $returned_consumer_tag, $consumer_tag, "Got a consumer_tag $returned_consumer_tag" );
}

# Test for deleting a queue
{
    local $@;
    my $deleted_queue = $ch->queue_delete( $queue, {} );
    is( $deleted_queue, $queue, "Deleted a queue $deleted_queue" );
}

is( $mq->channel_close($channel), 1, "Closed a channel $channel" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
