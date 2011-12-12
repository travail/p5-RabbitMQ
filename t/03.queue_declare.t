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

my $qname          = 'task_queue';
my $declared_qname = eval {
    $ch->queue_declare( $qname,
        { passive => 0, durable => 0, exclusive => 0, auto_delete => 1 } );
};
is( $declared_qname, $qname, "Declared a queue as $declared_qname" );

# Test for declaring q queue which is aleady exists with settting 1 to passive
{
    local $@;
    $declared_qname = eval {
        $ch->queue_declare( $qname,
            { passive => 1, durable => 1, exclusive => 0, auto_delete => 1 }
        );
    };
    is( $declared_qname, $qname,
        "Declared an existing queue as $declared_qname" );
}

# Test for declaring a queue which is already exists
{
    local $@;
    $declared_qname = eval {
        $ch->queue_declare( $qname,
            { passive => 0, durable => 1, exclusive => 0, autu_delete => 1 }
        );
    };
    isnt( $@, '', "Could not declare a queue $qname" );
}

is( $mq->channel_close($channel), 1, "Closed channel $channel" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
