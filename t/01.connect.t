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

isa_ok( $mq, "RabbitMQ", "Created RabbitMQ object" );
is( $sockfd, 1, "Logged in to " . HOST . ":" . PORT );
is( $mq->disconnect, 0, "Disconnected to " . HOST . ":" . PORT );

done_testing;
