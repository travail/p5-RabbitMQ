use Test::More;
use RabbitMQ;
use Data::Dumper;

my $mq     = RabbitMQ->new({timeout => 1});
my $sockfd = $mq->connect(
    {
        host     => '192.168.1.1',
        port     => 5672,
        user     => 'guest',
        password => 'guest',
        vhost    => '/',
    }
);
is(ref $mq, "RabbitMQ", "A RabbitMQ object");
is( $sockfd, 1, "Logged in rabbitmq-server as guest" );
$mq->disconnect;

done_testing;
