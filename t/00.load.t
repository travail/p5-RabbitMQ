use Test::More tests => 1;

BEGIN {
use_ok( 'RabbitMQ' );
}

diag( "Testing RabbitMQ $RabbitMQ::VERSION" );
