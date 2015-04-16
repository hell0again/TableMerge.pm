package TableMerge::Resolve;
use 5.008001;
use strict;
use warnings;
use utf8;

use Encode;
use File::Basename qw/dirname basename/;
use File::Path qw/mkpath/;
use File::Slurp;
use File::Temp qw/tempdir/;
use Getopt::Long;
use JSON::XS;
use Text::CSV_XS;

use TableMerge::Agent::James;

our $VERSION = "0.01";

sub new {
    my ($class) = @_;
    return bless +{}, $class;
}
sub run {
    my ($self, @args) = @_;
    my @commands;
    my $p = Getopt::Long::Parser->new(
        config => [ "posix_default", "no_ignore_case", "gnu_compat" ],
    );
    $p->getoptionsfromarray(
        \@args,
        #"h|help"    => sub { unshift @commands, 'help' },
        #"v|version" => sub { unshift @commands, 'version' },
        "a|agent" => sub { self->set_agent($_[1]) },
        "h|header" => sub { self->set_header_str($_[1]) },
    );
    push(@commands, @args);

    $self->{resolved} = shift @commands;

    if (! $self->{agent}) {
        $self->set_agent(TableMerge::Agent::James->new());
    }
    $self->resolve();
    print $self->{out};
    exit $self->{status};
}
sub set_agent {
    my ($self, $agent) = @_;
    $self->{agent} = $agent;
}
sub set_header_str {
    my ($self, $header_str) = @_;
    $header_str =~ s/\x0D?\x0A?$//;
    my @header = split(/,\s/, $header_str);
    $self->{header} = \@header;
}
sub resolve {
    my ($self) = @_;
    my $resolved = $self->{resolved};
    my $json = File::Slurp::read_file($resolved);
    my $agent = $self->{agent};
    my $csv = $agent->revert_json_to_csv($json, $self->{header});
    $self->{out} = $csv;
    $self->{status} = 0;
}

1;
__END__


