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
use UNIVERSAL::require;

our $VERSION = "0.03";

our $DEFAULT_AGENT = "TableMerge::Agent::James";
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
        "i|in-place" => sub { $self->set_inplace(1) },
        "a|agent" => sub { self->set_agent($_[1]) },
        "h|header" => sub { self->set_header_str($_[1]) },
    );
    push(@commands, @args);

    $self->{resolved} = shift @commands;

    if (! $self->{agent}) {
        $self->set_agent($DEFAULT_AGENT);
    }

    my $res = $self->resolve();
    if ($self->{inplace}) {
        open(my $out, ">", $self->{resolved}) or die sprintf "open % failed", $self->{resolved};
        print $out $res;
        close($out);
    } else {
        print STDOUT $res;
    }
    exit $self->{cmd_status};
}
sub set_agent {
    my ($self, $agent_class) = @_;
    $agent_class->require or die "can't load agent $agent_class", $@;
    $self->{agent} = $agent_class->new();
}
sub set_inplace {
    my ($self, $is_inplace) = @_;
    $self->{inplace} = $is_inplace;
}
sub set_header_str {
    my ($self, $header_str) = @_;
    $header_str =~ s/\x0D?\x0A?$//;
    my @header = split(/,\s/, $header_str);
    $self->{header} = \@header;
}
sub resolve {
    my ($self) = @_;
    my $resolved_path = $self->{resolved};
    my $resolved = File::Slurp::read_file($resolved_path);
    my $agent = $self->{agent};

    my $merged = $agent->decode_merged($resolved);
    ## TODO: decodeに失敗するケース
    $self->{cmd_status} = 0;
    $merged = $agent->post_merge_rows($merged);
    return encode('utf-8', $agent->decode_rows($merged));
}

1;
__END__


