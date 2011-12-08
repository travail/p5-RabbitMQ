use Test::More;

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

is( ref $mq, "RabbitMQ", "Created RabbitMQ object" );
is( $sockfd, 1, "Logged in to $host:$port" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
