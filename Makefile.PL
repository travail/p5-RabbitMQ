use inc::Module::Install;
use Getopt::Long;
use Pod::Usage;

my @module = qw(
    Module::Install::AuthorTests
    Module::Install::XSUtil
);
my @missing;
foreach my $module (@module) {
    eval "require $module";
    push @missing, $module if $@;
}
if (@missing) {
    print STDERR <<EOM;
*** Missing Modules ***

EOM
    foreach my $module (@missing) {
        print STDERR "  * $module\n";
    }
    exit 0;
}

my $opt = {
    rabbitmq_user     => 'guest',
    rabbitmq_password => 'guest',
    rabbitmq_host     => 'localhost',
    rabbitmq_port     => 5672,
    rabbitmq_vhost    => '/',
};
GetOptions(
    $opt,
    'rabbitmq_lib=s',
    'rabbitmq_inc=s',
    'rabbitmq_user=s',
    'rabbitmq_password=s',
    'rabbitmq_host=s',
    'rabbitmq_port=i',
    'rabbitmq_vhost=s',
    'h|help'
);
pod2usage(1) if $opt->{help};
gen_config($opt);

name 'RabbitMQ';
all_from 'lib/RabbitMQ.pm';

test_requires 'Test::More';

tests 't/*.t';
#recursive_author_tests('xt');

use_ppport;
cc_warnings;
cc_include_paths $opt->{rabbitmq_inc} if $opt->{rabbitmq_inc};
cc_libs "-L" . $opt->{rabbitmq_lib} if $opt->{rabbitmq_lib};
cc_libs '-lrabbitmq';

WriteAll;

sub gen_config {
    my $opt = shift;

    my $file = File::Spec->catfile( 't', 'config.pl' );
    open my $fh, '>', $file or die "Could not open file $file: $!";
    print $fh <<EOM;
\$rabbitmq_config = {
    user     => \'$opt->{rabbitmq_user}\',
    password => \'$opt->{rabbitmq_password}\',
    host     => \'$opt->{rabbitmq_host}\',
    port     => $opt->{rabbitmq_port},
    vhost    => \'$opt->{rabbitmq_vhost}\',
};
EOM
}

__END__

use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'RabbitMQ',
    VERSION_FROM      => 'lib/RabbitMQ.pm',
    PREREQ_PM         => {},
    ABSTRACT_FROM     => 'lib/RabbitMQ.pm',
    LIBS              => ['-L/usr/local/lib -lrabbitmq'],
    DEFINE            => '',
    INC               => '-I.',
    OBJECT            => '$(O_FILES)',
    OPTIMIZE          => '-g',
);
if ( eval { require ExtUtils::Constant; 1 } ) {
    my @names = (qw());
    ExtUtils::Constant::WriteConstants(
        NAME         => 'RabbitMQ',
        NAMES        => \@names,
        DEFAULT_TYPE => 'IV',
        C_FILE       => 'const-c.inc',
        XS_FILE      => 'const-xs.inc',
    );

}
else {
    use File::Copy;
    use File::Spec;
    foreach my $file ( 'const-c.inc', 'const-xs.inc' ) {
        my $fallback = File::Spec->catfile( 'fallback', $file );
        copy( $fallback, $file ) or die "Can't copy $fallback to $file: $!";
    }
}
