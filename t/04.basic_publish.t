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

my $queue          = 'basic_publish';
my $declared_queue = $ch->queue_declare( $queue,
    { passive => 0, durable => 0, exclusive => 0, auto_delete => 0 } );
is( $declared_queue->{queue}, $queue, "Declared a queue as " . $declared_queue->{queue} );

my $res = eval {
    $ch->basic_publish(
        {
            exchange   => '',
            routingkey => $queue,
            body       => 'hello world',
            props      => {
                content_type     => 'text/plain',
                content_encoding => 'UTF-8',
                correlation_id   => '123',
                reply_to         => 'reply_to',
                expiration       => '3600',
                message_id       => '10',
                type             => 'type',
                user_id          => 'guest',
                app_id           => 'app_id',
                delivery_mode    => 2,
                prioriry         => 1,
                timestamp        => time,
            },
            mandatory => 0,
            immediate => 0,
        }
    );
};

# Test for deleting a queue
my $deleted_queue = $ch->queue_delete( $queue, {} );
is( $deleted_queue->{queue}, $queue, "Deleted a queue $deleted_queue" );

is( $res, 0, 'Published a message' );
is( $mq->channel_close($channel), 1, "Closed channel $channel" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
