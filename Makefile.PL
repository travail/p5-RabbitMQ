use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'RabbitMQ',
    VERSION_FROM      => 'lib/RabbitMQ.pm',
    PREREQ_PM         => {},
    ABSTRACT_FROM     => 'lib/RabbitMQ.pm',
    LIBS              => ['-lrabbitmq'],
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
