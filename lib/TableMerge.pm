package TableMerge;
use 5.008001;
use strict;
use warnings;

use Encode;
use File::Basename qw/dirname basename/;
use File::Path qw/mkpath/;
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
    );
    push(@commands, @args);

    $self->{ours} = shift @commands;
    $self->{base} = shift @commands;
    $self->{theirs} = shift @commands;

    if (! $self->{agent}) {
        $self->set_agent(TableMerge::Agent::James->new());
    }
    print $self->merge();
    exit $self->{cmd_status};
}
sub set_agent {
    my ($self, $agent) = @_;
    $self->{agent} = $agent;
}
sub read_csv { ## 2次元配列を返す
    my ($self, $csv_path) = @_;
    my @lines = read_file($csv_path);
    my $rows = [];
    # my $eol = detect_linebreak($lines[0]);
    my $eol = $/;
    my $csv = Text::CSV_XS->new({binary => 1, eol => $eol});
    my @row_buff;
    while (my $row = shift @lines) {
        push(@row_buff, $row);
        my $st = $csv->parse(join("", @row_buff));
        if ($st) {
            my @fields = $csv->fields();
            push(@$rows, \@fields);
            @row_buff = ();
        }
    }
    if (0 < scalar @row_buff) {
        die "malformed csv";
    }
    return $rows;
}
sub detect_linebreak {
    my ($line) = @_;
    if ($line =~ /\r\n$/) {
        return "\r\n";
    } elsif ($line =~ /\r$/) {
        return "\r";
    } elsif ($line =~ /\n$/) {
        return "\n";
    }
}
sub temp_header {
    my ($self, $rows1, $rows2, $rows3) = @_;

}
sub merge {
    my ($self) = @_;
    my $ours_path = $self->{ours};
    my $base_path = $self->{base};
    my $theirs_path = $self->{theirs};
    my $agent = $self->{agent};
    $agent->{context} = +{
        filename => basename($theirs_path)
    };

    ## 読み込み(ソースがlist of row hashの場合、list of row listに変換する。
    my $rows1 = $self->read_csv($ours_path);
    my $rows2 = $self->read_csv($base_path);
    my $rows3 = $self->read_csv($theirs_path);

    ## 全体を舐めて事前処理。ヘッダ処理、メタデータ生成、PK推定など
    my $pre_parsed = $agent->pre_parse_rows($rows1, $rows2, $rows3);
    ## 個別のrowを処理
    @$rows1 = map {
        my $row = $_;
        $agent->parse_row($row, "ours", $pre_parsed);
    } @$rows1;
    @$rows2 = map {
        my $row = $_;
        $agent->parse_row($row, "base", $pre_parsed);
    } @$rows2;
    @$rows3 = map {
        my $row = $_;
        $agent->parse_row($row, "theirs", $pre_parsed);
    } @$rows3;
    ## 処理後のrowsを処理。事前マージなど
    ($rows1, $rows2, $rows3) =
        $agent->post_parse_rows($rows1, $rows2, $rows3, $pre_parsed);

    ## mkdir and write to file
    my $write_p = sub {
        my ($path, $str) = @_;
        mkpath dirname($path);
        open(my $out, "> $path");
        print $out encode('utf-8', $str);
        close $out;
    };
    my $tmp_dir = tempdir( CLEANUP => 1 );
    my $tmp1_path = File::Spec->catdir($tmp_dir, "ours", $ours_path);
    my $tmp2_path = File::Spec->catdir($tmp_dir, "base", $base_path);
    my $tmp3_path = File::Spec->catdir($tmp_dir, "theirs", $theirs_path);
    &$write_p($tmp1_path, $agent->pp_rows($rows1));
    &$write_p($tmp2_path, $agent->pp_rows($rows2));
    &$write_p($tmp3_path, $agent->pp_rows($rows3));

    ## merge
    $self->{cmd} = join(" ",
        "diff3", "-m",
        "-L", $ours_path,
        "-L", $base_path,
        "-L", $theirs_path,
        $tmp1_path, $tmp2_path, $tmp3_path,
    );
    my $diff3_res = `$self->{cmd}`;
    my $diff3_st  = $? >>8;
    # $self->{cmd_result} = $diff3_res;
    $self->{cmd_status} = $diff3_st;
    if ($diff3_st == 0) {
        return $agent->revert_json_to_csv($diff3_res);
    } else {
        return $diff3_res;
    }
}


1;
__END__

=encoding utf-8

=head1 NAME

TableMerge - Merge table files (csv) using 3-way merge

=head1 SYNOPSIS

    > cpanm git@github.com:hell0again/TableMerge.pm.git
    > cd sample/003
    > tablemerge ours.csv base.csv theirs.csv

    after resolve conflict..

    > tablemergeresolve out

=head1 DESCRIPTION

TableMerge is ...

=head1 LICENSE

Copyright (C) noname.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

noname E<lt>noname@example.comE<gt>

=cut

