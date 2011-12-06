use Test::More;
use RabbitMQ;
use Data::Dumper;

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
my $ch      = 5532;
my $channel = $mq->channel_open($ch);

is( ref $channel, "RabbitMQ::Channel", "Created RabbitMQ::Channel object" );
is( $channel->channel, $ch, "Opened channel $ch" );
is( $mq->channel_close($ch), 1, "Closed channel $ch" );
is( $mq->disconnect, 0, "Disconnected to $host:$port" );

done_testing;
