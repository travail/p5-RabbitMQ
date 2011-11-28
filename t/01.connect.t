use Test::More tests => 1;
use Data::Dumper;

BEGIN {
    use_ok( 'RabbitMQ' );
}

my $mq = RabbitMQ->new;
$mq->connect({
    host     => 'localhost',
    port     => 5672,
    user     => 'guest',
    password => 'guest',
    vhost    => '/',
});