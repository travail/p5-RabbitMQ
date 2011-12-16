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

my $queue   = 'task_queue';
my $channel = 5532;
my $ch = eval { $mq->channel_open($channel) };
isa_ok( $ch, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $ch->channel, $channel, "Opened channel $channel" );

# Test for declaring a queue
{
    my $declared_queue = eval {
        $ch->queue_declare( $queue,
            { passive => 0, durable => 0, exclusive => 0, auto_delete => 1 }
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

# Test for declaring a queue which is already exists
{
    local $@;
    my $declared_queue = eval {
        $ch->queue_declare( $queue,
            { passive => 0, durable => 1, exclusive => 0, autu_delete => 1 }
        );
    };
    isnt( $@, '', "Could not declare a queue $queue" );
}

is( $mq->channel_close($channel), 1, "Closed channel $channel" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
