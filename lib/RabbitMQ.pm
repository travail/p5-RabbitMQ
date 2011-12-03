package RabbitMQ;

use 5.008008;
use strict;
use warnings;
use AutoLoader;

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('RabbitMQ', $VERSION);

sub new {
    my ( $class, $args ) = @_;

    return $class->xs_new();
}

1;
__END__

=head1 NAME

RabbitMQ - Perl extension for blah blah blah

=head1 SYNOPSIS

  use RabbitMQ;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for RabbitMQ, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head1 METHODS

=head2 new

=head2 xs_new

=head2 connect

=head2 disconnect


=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

travail, E<lt>travail@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by travail

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
