package TableMerge::Agent::James;
use strict;
use warnings;
use utf8;
use Digest::SHA1 qw/sha1/;
use Encode;
use JSON::XS;

sub new {
    my ($class, %options) = @_;
    bless(+{
        comment_prefix => ":",
        id_prefix_tail => ":",
        id_delimiter => "\o{034}",
        id_kv_delimiter => ":",
        kv_delimiter => "=",
        #out_csv_eol => $\,
        #out_csv_eol => "\012",
        out_csv_eol => "\015\012",
        %options
    }, $class);
}
sub is_no_header {
    0;
}
sub generate_row_lines {
    my ($self, $row_hash, $header) = @_;
    my @_array;
    my @x_array;
    my $rid = $self->generate_row_id($row_hash, $header);
    ## 全カラム取り込み
    map {
        my $key = $_;
        push(@_array, $self->encode_line($rid, $key, $row_hash->{$key}));
    } @$header;
    # } keys %$row_hash;
    ## 追加カラムの取り込み(TODO 削除予定)
    my $extra_defaults = +{
        "test.csv" => {
            test_id => 0,
        },
    };
    map {
        my $column_defaults = $extra_defaults->{$_};
        map {
            my $key = $_;
            if (exists $row_hash->{$key}) {
            } else {
                push(@_array, $self->encode_line($rid, $key, $column_defaults->{$key}));
            }
        } keys %$column_defaults;
    } grep { $self->{context}{filename} =~ /$_/ } keys %$extra_defaults;
    ## カラム順序を一旦ソート
    # @_array = sort {$a cmp $b} @_array;
    # push(@x_array, $self->encode_row_digest($row_hash));

    ## ロー同士のソートキーをローの先頭に仕込み
    push(@x_array, $self->encode_header_line($rid));
    ## おまじない
    map {
        my $line = $_;
        $line =~ /^([^$self->{kv_delimiter}]*)/;
        my $key = $1;
        push(@x_array, join("",
            $self->{comment_prefix}, "//", $key
        ));
        push(@x_array, $line);
        push(@x_array, join("",
            $self->{comment_prefix}, "//", $key
        ));
    } @_array;
    return \@x_array;
}
sub get_row_id_from_row_lines {
    my ($self, $x_array) = @_;
    my $head_line = $x_array->[0];
    my $row_id_prefix_head = 0;
    my $row_id_prefix_tail = index($head_line, $self->{id_prefix_tail}, $row_id_prefix_head + 1);
    return substr($head_line, $row_id_prefix_head + 1, $row_id_prefix_tail - 1);
}
# idがあればid、他は先頭2カラムを採用
# FIXME: 実際に一意なのかを確認する、自動で選択する
sub generate_row_id {
    my ($self, $row_hash, $header) = @_;
    my $source = +{
        by_index => [],
        by_name => [],
    };
    my $extras = +{
        "test1.csv" => +{
            "by_index" => [0,1,2],
        },
        "test2.csv" => {
            "by_index" => [0,1],
        }
    };
    map {
        my $h = $extras->{$_};
        if (exists $h->{by_index}) {
            $source->{by_index} = $h->{by_index};
        } elsif (exists $h->{by_name}) {
            $source->{by_name} = $h->{by_name};
        }
    } grep { $self->{context}{filename} =~ /$_/ } keys %$extras;
    if (scalar(@{ $source->{by_index} }) || scalar(@{ $source->{by_name} })) {
    } elsif (exists $row_hash->{id}) {
        $source->{by_name} = ["id"];
    } else {
        $source->{by_index} = [0,1];
    }

    my $row_id;
    if (0 < scalar(@{ $source->{by_name} })) {
        $row_id = join($self->{id_delimiter},
            map {
                $row_hash->{ $_ }
            } @{$source->{by_name}}
        );
    } else {
        $row_id = join($self->{id_delimiter},
            map {
                $row_hash->{ $header->[$_] }
            } @{$source->{by_index}}
        );
    }
    return $row_id;
}
sub encode_row_digest {
    my ($self, $row_hash) = @_;
    my $sha1 = Digest::SHA1->new;
    $sha1->add(
        join("--", map {
            sprintf("%s__%s", $_, encode('utf-8', $row_hash->{$_}));
        } sort { $a cmp $b } keys %$row_hash)
    );
    my $digest = $sha1->hexdigest;
    return join("", (
        $self->{comment_prefix},
        $digest
    ));
}
sub encode_line {
    my ($self, $rid, $key, $val) = @_;
    return join("", (
        $rid, $self->{id_kv_delimiter},
        $key, $self->{kv_delimiter},
        (defined $val)? $val: ""
    )); ## e.g. 3010:hp=12
}
sub decode_line {
    my ($self, $line) = @_;
    my ($comment, $prefix, $key, $val);
    my $ret = {};
    my $comment_head = index($line, $self->{comment_prefix});
    if (0 == $comment_head) {
        $ret->{comment} = substr($line, $comment_head +1);
    } else {
        my $idx_id_kv_delimiter = index($line, $self->{id_kv_delimiter});
        my $idx_kv_delimiter = index($line, $self->{kv_delimiter}, $idx_id_kv_delimiter +1);
        $ret->{row_id} = substr($line, 0, $idx_id_kv_delimiter -1);
        $ret->{key} = substr($line, $idx_id_kv_delimiter +1, $idx_kv_delimiter - $idx_id_kv_delimiter -1);
        $ret->{value} = substr($line, $idx_kv_delimiter +1);
    }
    return $ret;
}
sub encode_header_line {
    my ($self, $rid) = @_;
    my $sha1 = Digest::SHA1->new;
    return join("", (
        $self->{comment_prefix},
        $sha1->add($rid)->hexdigest,
        $self->{id_prefix_tail},
        $rid
    ));
}
sub to_rows_sorted {
    my ($self, $rows_map) = @_;
    my @sorted = map {
        $rows_map->{$_}
    } sort {
        my @as = split(/[\:$self->{id_delimiter}]/, $a);
        my @bs = split(/[\:$self->{id_delimiter}]/, $b);
        my $r = 0;
        while($r == 0 && scalar @as > 0) {
            my $x = shift @as;
            my $y = shift @bs;
            $r = $r || ($x =~ /^\d+$/ && $y =~ /^\d+$/) ?
                $x <=> $y:
                $x cmp $y;
        }
        $r;
    } keys %$rows_map;
    return \@sorted;
}

sub sort_rows_for_view {
    my ($self, $rows) = @_;
    my @sorted = sort {
        my @as = split(/[\:$self->{id_delimiter}]/, $a->[0]);
        my @bs = split(/[\:$self->{id_delimiter}]/, $b->[0]);
        my $r = 0;
        shift @as; # TODO: コメント行で id_delimiterとコメントマークが被ってるので2回必要
        shift @as;
        shift @bs;
        shift @bs;
        while($r == 0 && scalar @as > 0) {
            my $x = shift @as;
            my $y = shift @bs;
            $r = $r || ($x =~ /^\d+$/ && $y =~ /^\d+$/) ?
                $x <=> $y:
                $x cmp $y;
        }
        $r;
    } @$rows;
    return \@sorted;
}
sub decode_row { # マージ用フォーマットからcsv用のリストを返す
    # TODO: いいかんじにカラムを並べているはずなのでheaderは基本不要
    my ($self, $row, $header) = @_;
    my $_row_hash = +{};
    my $_header_hash = +{};
    my $row_array = [];
    my $header_array = [];

    my $header_from_row = ($header && 0 < scalar(@$header))? 0 : 1;
    map {
        my $line_str = $_;
        my $line_obj = $self->decode_line($line_str);
        if (exists $line_obj->{comment}) {
        } else {
            my $key = $line_obj->{key};
            if (exists $_row_hash->{$key}) {
                die "Error duplicate entry";
            }
            $_row_hash->{$key} = $line_obj->{value};
            $_header_hash->{$key} = $key;
            if ($header_from_row) {
                push(@$row_array, $line_obj->{value});
                push(@$header_array, $key);
            }
        }
    } @$row;
    if (! $header_from_row) {
        map {
            push(@$row_array, $_row_hash->{$_});
            push(@$header_array, $_header_hash->{$_});
        } @$header;
    }
    return +{
        header => $header_array,
        row => $row_array,
    };
}
sub decode_rows {
    my ($self, $rows, $header) = @_;
    my $res = [];
    if ($header && 0 < scalar(@$header)) {
    } else {
        my $sample_row = $rows->[0];
        my $decoded = $self->decode_row($sample_row);
        $header = $decoded->{header};
    }
    push(@$res, $header);
    map {
        my $row = $_;
        # my $decoded = $self->decode_row($row, $header);
        my $decoded = $self->decode_row($row);
        push(@$res, $decoded->{row});
    } @$rows;
    return $res;
}
sub revert_json_to_csv {
    my ($self, $json_str, $header) = @_;
    # TODO: カラムオーダーはいい感じになってるはずなので $headerは基本不要

    my $merged = decode_json($json_str);
    $merged = $self->sort_rows_for_view($merged);
    $merged = $self->decode_rows($merged, $header);
    my $csv = Text::CSV_XS->new({binary => 1, eol => $self->{out_csv_eol}});
    # open(my $out_csv, "> $tmp_merged_csv_path"); # debug
    return join("", map {
        my $row = $_;
        $csv->combine(@$row);
        encode('utf-8', $csv->string());
        #my $ln = encode('utf-8', $csv->string());
        # print $out_csv $ln; # debug
        #print $ln;
    } @$merged);
    # close $out_csv; # debug
}

1;

