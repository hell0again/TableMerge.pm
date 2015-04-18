package TableMerge::Agent::Base;
use strict;
use warnings;
use utf8;
use JSON::XS;
use Text::CSV_XS;

sub new {
    my ($class, %options) = @_;
    return bless(+{
        out_csv_eol => "\n",
    }, $class);
}

########################
sub parse_source {
    my ($self, $source_lines) = @_;
    my $rows = [];
    my $csv = Text::CSV_XS->new({binary => 1, allow_whitespace => 1, eol => $self->{out_csv_eol}});
    my $len = undef;
    while (my $row = shift @$source_lines) {
        my $st = $csv->parse($row);
        if ($st) {
            my @fields = $csv->fields();
            if (defined $len && $len != scalar @fields) {
                die "column length mismatch";
            } else {
                $len = scalar @fields;
            }
            push(@$rows, \@fields);
        } else {
            die "parse failed";
        }
    }
    return $rows;
}
########################
sub pre_parse_rows {
    my ($self, $rows1, $rows2, $rows3) = @_;
    return +{};
}
########################
sub parse_row {
    my ($self, $row, $label, $pre_parsed) = @_;
    return $row;
}
########################
sub post_parse_rows {
    my ($self, $rows_lines1, $rows_lines2, $rows_lines3, $pre_parsed) = @_;
    return ($rows_lines1, $rows_lines2, $rows_lines3);
}
########################
sub pp_rows {
    my ($self, $rows) = @_;
    my $json = new JSON::XS;
    $json->pretty;
    return $json->encode($rows);
}
########################
sub decode_merged {
    my ($self, $json_str) = @_;
    my $json = new JSON::XS;
    $json->pretty;
    return $json->decode($json_str);
}
########################
sub post_merge_rows {
    my ($self, $merged_rows) = @_;
    return $merged_rows;
}
########################
sub decode_rows {
    my ($self, $rows) = @_;
    my $csv = Text::CSV_XS->new({binary => 1, allow_whitespace => 1, eol => $self->{out_csv_eol}});
    return join("", map {
        my $row = $_;
        $csv->combine(@$row);
        $csv->string();
    } @$rows);
}

1;

