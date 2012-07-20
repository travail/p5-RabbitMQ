#!/usr/bin/env perl

use strict;
use FindBin ();
use lib "$FindBin::Bin/lib";
use Test::More;
use Test::RabbitMQ::Config;

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

my $queue = 'basic_get';
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

# Test for publishing
my $exchange   = '';
my $routing_key = $queue;
my $body       = 'hello world';
my $mandatory  = 0;
my $immediate  = 0;
my $props      = {
    content_type     => 'text/plain',
    content_encoding => 'UTF-8',
    correlation_id   => '123',
    reply_to         => 'reply_to',
    expiration       => '3600',
    message_id       => 'message_id',
    type             => 'type',
    user_id          => 'guest',
    app_id           => 'app_id',
    delivery_mode    => 2,
    priority         => 1,
    timestamp        => time,
};
{
    my $res = eval {
        $ch->basic_publish(
            {
                exchange   => $exchange,
                routing_key => $routing_key,
                body       => $body,
                props      => $props,
                mandatory  => $mandatory,
                immediate  => $immediate,
            }
        );
    };
    is( $res, 0, "Published a message" );
}

# Test for getting
{
    local $@;
    my $res = eval { $ch->basic_get( $queue, { no_ack => 1 } ) };
    ok( ref $res, "Got a message with queue $queue" );
    is( $res->{exchange}, $exchange, "The exchange in response is $exchange" );
    is( $res->{routing_key}, $routing_key, "The routing_key in response is $routing_key" );
    is( $res->{body}, $body, "The body in response is $body" );
    is( $res->{props}->{content_type}, $props->{content_type},
        "The content_type in response is " . $props->{content_type} );
    is( $res->{props}->{content_encoding}, $props->{content_encoding},
        "The content_encoding in response is " . $props->{content_encoding} );
    is( $res->{props}->{correlation_id}, $props->{correlation_id},
        "The correlation_id in response is " . $props->{correlation_id} );
    is( $res->{props}->{reply_to}, $props->{reply_to},
        "The reply_to in response is " . $props->{reply_to} );
    is( $res->{props}->{expiration}, $props->{expiration},
        "The expiration in response is " . $props->{expiration} );
    is( $res->{props}->{message_id}, $props->{message_id},
        "The message_id in response is " . $props->{message_id} );
    is( $res->{props}->{type}, $props->{type},
        "The type in response is " . $props->{type} );
    is( $res->{props}->{user_id}, $props->{user_id},
        "The user_id in response is " . $props->{user_id} );
    is( $res->{props}->{app_id}, $props->{app_id},
        "The app_id in response is " . $props->{app_id} );
    is( $res->{props}->{delivery_mode}, $props->{delivery_mode},
        "The delivery_mode in response is " . $props->{delivery_mode} );
    is( $res->{props}->{priority}, $props->{priority},
        "The priority in response is " . $props->{priority} );
    is( $res->{props}->{timestamp}, $props->{timestamp},
        "The timestamp in response is " . $props->{timestamp} );
}

# Test for deleting a queue
{
    local $@;
    my $deleted_queue = $ch->queue_delete( $queue, {} );
    is( $deleted_queue->{queue}, $queue, "Deleted a queue " . $deleted_queue->{queue} );
}

is( $mq->channel_close($channel), 1, "Closed a channel $channel" );
is( $mq->disconnect, 0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
