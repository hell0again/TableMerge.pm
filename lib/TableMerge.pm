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
    $self->merge();
    print $self->{out};
    exit $self->{status};
}
sub set_agent {
    my ($self, $agent) = @_;
    $self->{agent} = $agent;
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

    my $csv = Text::CSV_XS->new({binary => 1, eol => $/});
    open my $io1, "<", $ours_path;
    open my $io2, "<", $base_path;
    open my $io3, "<", $theirs_path;
    my (@row_buff1, @row_buff2, @row_buff3);
    my ($map1, $map2, $map3) = ({}, {}, {});
    my ($digest1, $digest2, $digest3) = ({}, {}, {});
    my ($header1, $header2, $header3, $merged_header);

    my $row1 = $csv->getline($io1) || [];
    my $row2 = $csv->getline($io2) || [];
    my $row3 = $csv->getline($io3) || [];
    if ($agent->is_no_header) {
        die "agent option(is_no_header) implemented!"
    } else {
        my %header_hash;
        @header_hash{@$row1} = ();
        @header_hash{@$row2} = ();
        @header_hash{@$row3} = ();
        $header1 = $row1;
        $header2 = $row2;
        $header3 = $row3;

        my $i = scalar(@$row1);
        my $header_order = {};
        map {
            $header_order->{$_} = $i--;
        } @$row1;
        @$merged_header = sort {
            ($header_order->{$b} || 0) <=> ($header_order->{$a} || 0)
        } keys %header_hash;

        $row1 = $csv->getline($io1);
        $row2 = $csv->getline($io2);
        $row3 = $csv->getline($io3);
    }
    while ($row1) {
        my %row_hash;
        @row_hash{ @$header1 } = @$row1;
        my $x = $agent->generate_row_lines(\%row_hash, $merged_header);
        my $row_id = $agent->get_row_id_from_row_lines($x);
        $map1->{$row_id} = $x;
        $digest1->{$row_id} = $agent->encode_row_digest(\%row_hash);
        $row1 = $csv->getline($io1); # //while
    }
    close $io1;
    while ($row2) {
        my %row_hash;
        @row_hash{ @$header2 } = @$row2;
        my $x = $agent->generate_row_lines(\%row_hash, $merged_header);
        my $row_id = $agent->get_row_id_from_row_lines($x);
        $map2->{$row_id} = $x;
        $digest2->{$row_id} = $agent->encode_row_digest(\%row_hash);
        $row2 = $csv->getline($io2); # //while
    }
    close $io2;
    while ($row3) {
        my %row_hash;
        @row_hash{ @$header3 } = @$row3;
        my $x = $agent->generate_row_lines(\%row_hash, $merged_header);
        my $row_id = $agent->get_row_id_from_row_lines($x);
        $map3->{$row_id} = $x;
        $digest3->{$row_id} = $agent->encode_row_digest(\%row_hash);
        $row3 = $csv->getline($io3); # //while
    }
    close $io3;
    ## 事前に自明な分は先にマージ
    my $pre_merge = {};
    map { $pre_merge->{$_} = ($pre_merge->{$_} || 0) +1; } keys $map1;
    map { $pre_merge->{$_} = ($pre_merge->{$_} || 0) +10; } keys $map2;
    map { $pre_merge->{$_} = ($pre_merge->{$_} || 0) +100; } keys $map3;
    map { # ローがoursのみに存在(追加確定)
        $map3->{$_} = $map1->{$_};
        $map2->{$_} = $map1->{$_};
        $digest3->{$_} = $digest1->{$_};
        $digest2->{$_} = $digest1->{$_};
        $pre_merge->{$_} = 111;
    } grep {
        $pre_merge->{$_} == 1
    } keys %$pre_merge;
    map { # ローがtheirsのみに存在(追加確定)
        $map1->{$_} = $map3->{$_};
        $map2->{$_} = $map3->{$_};
        $digest1->{$_} = $digest3->{$_};
        $digest2->{$_} = $digest3->{$_};
        $pre_merge->{$_} = 111;
    } grep {
        $pre_merge->{$_} == 100
    } keys %$pre_merge;
    map { # ours, theirsのダイジェストが一致(追加or変更確定)
        $map2->{$_} = $map3->{$_};
        $digest2->{$_} = $digest3->{$_};
        $pre_merge->{$_} = 111;
    } grep {
        ($pre_merge->{$_} == 101 || $pre_merge->{$_} == 111) &&
        ($digest1->{$_} eq $digest3->{$_})
    } keys %$pre_merge;
    map { # baseのみに存在(削除確定)
        delete $map2->{$_};
        delete $digest2->{$_};
        delete $pre_merge->{$_};
    } grep {
        $pre_merge->{$_} == 10
    } keys %$pre_merge;

    my $rows_sorted1 = $agent->to_rows_sorted($map1);
    my $rows_sorted2 = $agent->to_rows_sorted($map2);
    my $rows_sorted3 = $agent->to_rows_sorted($map3);

    my $json = new JSON::XS;
    $json->pretty;
    my $tmp_dir = tempdir( CLEANUP => 1 );
    my $tmp_out1_path = File::Spec->catdir($tmp_dir, "a", $ours_path);
    my $tmp_out2_path = File::Spec->catdir($tmp_dir, "b", $base_path);
    my $tmp_out3_path = File::Spec->catdir($tmp_dir, "c", $theirs_path);
    my $tmp_merged_json_path = File::Spec->catdir($tmp_dir, "out.conflict.json");
    my $tmp_merged_csv_path  = File::Spec->catdir($tmp_dir, "out.csv");
    my $out_merged_json_path = $ours_path . ".conflict.json";
    mkpath dirname($tmp_out1_path);
    mkpath dirname($tmp_out2_path);
    mkpath dirname($tmp_out3_path);

    open(my $out1, "> $tmp_out1_path");
    print $out1 encode('utf-8', $json->encode($rows_sorted1));
    close $out1;
    open(my $out2, "> $tmp_out2_path");
    print $out2 encode('utf-8', $json->encode($rows_sorted2));
    close $out2;
    open(my $out3, "> $tmp_out3_path");
    print $out3 encode('utf-8', $json->encode($rows_sorted3));
    close $out3;

    my $diff3_cmd = join(" ",
        "diff3", "-m",
        "-L", $ours_path,
        "-L", $base_path,
        "-L", $theirs_path,
        $tmp_out1_path, $tmp_out2_path, $tmp_out3_path,
    );
    my $diff3_res = `$diff3_cmd`;
    my $diff3_st  = $? >>8;
    open(my $out_json, "> $tmp_merged_json_path");
    print $out_json $diff3_res;
    close $out_json;
    if ($diff3_st == 0) {
        #print $agent->revert_json_to_csv($diff3_res, $merged_header);
        $self->{out} =  $agent->revert_json_to_csv($diff3_res, $merged_header);
    } else {
        #print $diff3_res;
        $self->{out} = $diff3_res;
    }
    # return $diff3_st;
    $self->{status} = $diff3_st;
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

