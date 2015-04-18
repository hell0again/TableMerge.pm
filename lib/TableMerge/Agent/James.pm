package TableMerge::Agent::James;
use strict;
use warnings;
use utf8;
use Digest::SHA1;
use Encode;
use JSON::XS;
use List::Compare;
use List::Permutor;
use Text::CSV_XS;
use Text::Levenshtein::XS qw/distance/;

sub new {
    my ($class, %options) = @_;
    bless(+{

        comment_prefix => ":",
        id_hash_tail => ":",
        id_delimiter => "\o{034}",
        id_kv_delimiter => ":",
        kv_delimiter => "=",
        #out_csv_eol => $\,
        #out_csv_eol => "\012",
        out_csv_eol => "\015\012",
        no_header => 0,
        %options
    }, $class);
}
sub is_no_header {
    0;
}

########################
sub _merge_header {
    # ours, base, theirs
    # 111 : keep
    # 001 : keep (ours,baseにカラム追加)
    # 100 : keep (base,theirsにカラム追加)
    # 101 : keep (baseにカラム追加)
    # 010 : remove
    # 011 : keep?(lets conflict
    # 110 : keep?(lets conflict
    my($self, $header1, $header2, $header3) = @_;
    my $header_hash = {};
    map {
        $header_hash->{$_} = 0b100 | ($header_hash->{$_} || 0b000)
    } @$header1;
    map {
        $header_hash->{$_} = 0b010 | ($header_hash->{$_} || 0b000)
    } @$header2;
    map {
        $header_hash->{$_} = 0b001 | ($header_hash->{$_} || 0b000)
    } @$header3;
    map {
        $header_hash->{$_} = 0b000;
    } grep {
        $header_hash->{$_} == 0b010
    } keys %$header_hash;
    map {
        $header_hash->{$_} = 0b111;
    } grep {
        $header_hash->{$_} == 0b100 ||
        $header_hash->{$_} == 0b001 ||
        $header_hash->{$_} == 0b101
    } keys %$header_hash;

    my @merged_header = grep {
        $header_hash->{$_} > 0
    } keys %$header_hash;

    ## 必要なカラムを残してマージしたmerged_header
    my @ordered_merged = $self->_find_nearest_order_of_merged_header(\@merged_header, $header1, $header2, $header3);
    my @ordered_merged_header1 = grep {
        $header_hash->{$_} & 0b100
    } @ordered_merged;
    my @ordered_merged_header2 = grep {
        $header_hash->{$_} & 0b010
    } @ordered_merged;
    my @ordered_merged_header3 = grep {
        $header_hash->{$_} & 0b001
    } @ordered_merged;
    return (\@ordered_merged_header1, \@ordered_merged_header2, \@ordered_merged_header3);
}
# マージ済みヘッダで最もカラム移動が小さいものがほしい
sub _find_nearest_order_of_merged_header {
    my ($self, $merged_header, $header1, $header2, $header3) = @_;
    my @chars = qw|
        a b c d e f g h i j k l m n o p q r s t u v w x y z
        A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
        0 1 2 3 4 5 6 7 8 9 + /
    |;
    my $index_map = {};
    map {
        $index_map->{ $merged_header->[$_] } = $_;
    } 0 .. $#{$merged_header};
    my $_encode = sub {
        my @list = @_;
        return join "", map {
            $chars[ $index_map->{$_} ]
        } @list;
    };
    my $encoded_header1 = &$_encode(@header1);
    my $encoded_header2 = &$_encode(@header2);
    my $encoded_header3 = &$_encode(@header3);
    my $perm = new List::Permutor(@$merged_header);
    my @merged;
    my $min = 2 ** 15;
    while (my @candidate = $perm->next) {
        my $encoded_candidate = &$_encode(@candidate);
        my $lsd1 = distance($encoded_header1, $encoded_candidate);
        my $lsd2 = distance($encoded_header2, $encoded_candidate);
        my $lsd3 = distance($encoded_header3, $encoded_candidate);
        if ($lsd1 == 0 || $lsd2 == 0 || $lsd3 == 0) {
            @merged = @candidate;
            last;
        }
        my $p = $lsd1 * $lsd1 + $lsd2 * $lsd2 + $lsd3 * $lsd3;
        if ($p < $min) {
            $min = $p;
            @merged = @candidate;
        }
    }
    return @merged;
}
sub pre_parse_rows {
    my ($self, $rows1, $rows2, $rows3) = @_;

    ## ヘッダのマージ
    my ($alt_header1, $alt_header2, $alt_header3);
    if ($self->{no_header}) { ## assign temp name. suppose each rows has same format
        my $f = "col_%02d";
        my $sample1 = $rows1->[0];
        my $sample2 = $rows2->[0];
        my $sample3 = $rows3->[0];
        if (scalar(@$sample1) != scalar(@$sample2) || scalar(@$sample2) != scalar(@$sample3)) {
            die sprintf("column count mismatch! (1: %s, 2: %s, 3: %s)",
                scalar(@$sample1), scalar(@$sample2), scalar(@$sample3)
            );
        }
        @$alt_header1 = @$alt_header2 = @$alt_header3 = map {
            sprintf($f, $_);
        } $#{$sample1};
    } else {
        ($alt_header1, $alt_header2, $alt_header3) = $self->_merge_header($header1, $header2, $header3);
    }

    my $lc = List::Compare->new($alt_header1, $alt_header2);
    my @intersect = $lc->get_intersection;
    $lc = List::Compare->new(\@intersect, $alt_header3);
    @intersect = $lc->get_intersection;

    ## PK探し。各テーブルは共通のPKをもち、テーブルごとにPKでの一意性を満たしているものとする。
    my $pks = $self->find_pks(
        [$header1 $header2, $header3],
        [$rows1, $rows2, $rows3]
    );

    my $res = +{
        ours => {
            header => $alt_header1,
            org_header => $header1,
            pks => $pks,
        },
        base => {
            header => $alt_header2,
            org_header => $header2,
            pks => $pks,
        },
        theirs => {
            header => $alt_header3,
            org_header => $header3,
            pks => $pks,
        },
    };
    $res;
}
## すべてのテーブルが同じキーをもち、テーブルごとに一意性を満たしていること
## 一意キーに空白文字、NULLを含まないこと
## 見つからなかったら死ぬ
sub find_pks {
    my ($self, $header_list, $rows_list) = @_;
    my %cols;
    map {
        my $header = $_;
        @cols{ @$header } = ();
    } @$header_list;
    my @col_list = keys %cols;

    my $index_maps = {};
    for my $i (0 .. $#{$header_list}) {
        $index_maps->{$i} = +{};
        my $header = $header_list->[$i];
        map {
            $index_maps->{$i}{ $header->[$_] } = $_;
        } 0 .. $#{$header};
    }
    for my $len (1 .. scalar(@col_list)) {
        my $iter = combinations(\@col_list, $len);
        my $candidates = [];
        while (my $cand = $iter->next) {
            # next if grep { $_ =~ /[\s\t\n]/ } @$cand;
            my $is_candidate = 1;
            for my $i (0 .. $#{$header_list}) {
                my @cand_index = grep { defined $_ } map { $index_maps->{$i}{$_} } @$cand;
                if (scalar @cand_index != @$cand) {
                    $is_candidate = 0;
                    last; # for $i
                };
                my $rows = $rows_list->[$i];
                my $judge = $self->judge_pk($rows, \@cand_index);
                if (!defined $judge) {
                    $is_candidate = 0;
                    last; # for $i
                }
            }
            if ($is_candidate) {
                push(@$candidates, $cand);
            }
        }
        if (0 < scalar @$candidates) {
            @$candidates = sort {
                $b->{max_key_len} <=> $a->{max_key_len}
            } @$candidates;
            return shift @$candidates;
        }
    }
    return;
}
sub judge_pk {
    my ($self, $rows, $candidate_index_list) = @_;
    my $is_unique = 1;
    my $max_length = 0;

    my $key_map = +{};
    for my $row (@$rows) {
        my $key = join("\o{034}", map {
            $row->[$_];
        } @$candidate_index_list);
        if (exists $key_map->{$key}) {
            $is_unique = 0;
            last; # for $row
        } else {
            $key_map->{$key} = 1;
            my $len = length($key);
            if ($max_length < $len) {
                $max_length = $len;
            }
        }
    }
    my $row_num = scalar(@$rows);
    if ($row_num != scalar keys %$key_map) {
        $is_unique = 0;
    } ## aborted or founded candidate
    return ($is_unique)? +{
        indexes      => $candidate_index_list,
        max_key_len  => $max_length,
        row_num      => scalar(@$rows),
    }: undef;
}
########################
sub parse_row {
    my ($self, $row, $label, $pre_parsed) = @_;
    my $row_hash = {};
    my $org_header = $pre_parsed->{$label}{org_header};
    my $new_header = $pre_parsed->{$label}{header};
    my $pks = $pre_parsed->{$label}{pks};
    @$row_hash{ @$org_header } = @$row;

    my $row_id = $agent->generate_row_id($row_hash, $pks);
    my @x_array;
    push(@x_array, $self->encode_header_line($row_id));
    map {
        my $key = $_;
        push(@x_array, join("", (
            $self->{comment_prefix}, "//", $key
        ));
        push(@x_array, $self->encode_line($row_id, $key, $row_hash->{$key});
        push(@x_array, join("", (
            $self->{comment_prefix}, "//", $key
        ));
    } @$new_header;
    return \@x_array;
}
sub generate_row_id {
    my ($self, $row_hash, $pks) = @_;

    ## TODO: move to find_pks
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
        $source->{by_name} = $pks;
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
sub encode_header_line {
    my ($self, $row_id) = @_;
    my $sha1 = Digest::SHA1->new;
    return join("", (
        $self->{comment_prefix},
        $sha1->add($row_id)->hexdigest,
        $self->{id_hash_tail},
        $row_id
    ));
}
sub decode_header_line {
    my ($self, $header_line) = @_;
    my $row_id_hash_head = 1;
    my $row_id_hash_tail = index($header_line, $self->{id_hash_tail}, $row_id_hash_head);
    my $row_id_hash = substr($header_line, $row_id_hash_head, $row_id_hash_tail - $row_id_hash_head + 1);
    my $row_id_head = $row_id_hash_tail + 2;
    my $row_id = substr($header_line, $row_id_head);
    return +{
        row_id_hash => $row_id_hash,
        row_id => $row_id,
    }
}
sub encode_line {
    my ($self, $row_id, $key, $val) = @_;
    return join("", (
        $row_id, $self->{id_kv_delimiter},
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

## DELETE ME?
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
########################
sub post_parse_rows {
    my ($self, $rows_lines1, $rows_lines2, $rows_lines3, $pre_parsed) = @_;

    my ($rows_map1, $rows_map2, $rows_map3) = ({}, {}, {});
    my $pre_merge = {};

    map {
        my $row = $_;
        my $h = $self->decode_header_line($row->[0]);
        my $key = $h->{hash};
        $rows_map1->{$key} = $row;
        $pre_merge->{$key} = ($pre_merge->{$key} || 0) + 0b100;
    } @$rows_lines1;
    map {
        my $row = $_;
        my $h = $self->decode_header_line($row->[0]);
        my $key = $h->{hash};
        $rows_map2->{$key} = $row;
        $pre_merge->{$key} = ($pre_merge->{$key} || 0) + 0b010;
    } @$rows_lines2;
    map {
        my $row = $_;
        my $h = $self->decode_header_line($row->[0]);
        my $key = $h->{hash};
        $rows_map3->{$key} = $row;
        $pre_merge->{$key} = ($pre_merge->{$key} || 0) + 0b001;
    } @$rows_lines3;

    ## 自明なローについて事前にマージ
    map { # ローがoursのみに存在(追加確定)
        $pre_merge->{$_} = 0b111;
        $rows_map3->{$_} = $rows_map1->{$_};
        $rows_map2->{$_} = $rows_map1->{$_};
    } grep {
        $pre_merge->{$_} == 0b100
    } keys %$pre_merge;
    map { # ローがtheirsのみに存在(追加確定)
        $pre_merge->{$_} = 0b111;
        $rows_map1->{$_} = $rows_map3->{$_};
        $rows_map2->{$_} = $rows_map3->{$_};
    } grep {
        $pre_merge->{$_} == 0b001
    } keys %$pre_merge;
    map { # ours, theirsのダイジェストが一致(追加or変更確定)
        my $digest1 = $self->encode_row_digest($rows_map1->{$_});
        my $digest3 = $self->encode_row_digest($rows_map1->{$_});
        if ($digest1 eq $digest3) {
            $pre_merge->{$_} = 0b111;
            $rows_map2->{$_} = $rows_map3->{$_};
        }
    } grep {
        ($pre_merge->{$_} == 101 || $pre_merge->{$_} == 111)
    } keys %$pre_merge;
    map { # baseのみに存在(削除確定)
        delete $rows_map2->{$_};
    } grep {
        $pre_merge->{$_} == 0b010
    } keys %$pre_merge;

    ## ローの再ソート
    my $p1; @$p1 = values $rows_map1;
    $p1 = $self->sort_rows($p1, 0);
    my $p2; @$p2 = values $rows_map2;
    $p2 = $self->sort_rows($p2, 0);
    my $p3; @$p3 = values $rows_map3;
    $p3 = $self->sort_rows($p3, 0);

    return ($p1, $p2, $p3);
}
sub sort_rows {
    my ($self, $rows_lines, $skip_hash) = @_;
    my @sorted = map {
        $rows_map->{$_}
    } sort {
        my $a_st = $a->[0];
        my $b_st = $b->[0];
        my $dlms = join("",
            $self->{comment_prefix},
            $self->{id_hash_tail},
            $self->{id_kv_delimiter}
        );
        my @as = split(/[${dlms}]/, $a->[0]);
        my @bs = split(/[${dlms}]/, $b->[0]);
        my $r = 0;
        shift @as;
        shift @bs;
        if ($skip_hash) {
            shift @as;
            shift @bs;
        }
        while($r == 0 && scalar @as > 0) {
            my $x = shift @as;
            my $y = shift @bs;
            $r = ($x =~ /^\d+$/ && $y =~ /^\d+$/) ?
                $x <=> $y:
                $x cmp $y;
        }
        $r;
    } @$rows_lines;
    return \@sorted;
}

########################
sub pp_rows {
    my ($self, $rows) = @_;
    my $json = new JSON::XS;
    $json->pretty;
    return $json->encode($rows));
}
########################
sub revert_json_to_csv {
    my ($self, $json_str, $header) = @_;

    my $rows = decode_json($json_str);
    $rows = $self->sort_rows($rows, 1);

    my $csv = Text::CSV_XS->new({binary => 1, eol => $self->{out_csv_eol}});
    return join("", map {
        my $decoded = $self->decode_row($_);
        $csv->combine(@{$decoded->{rows}});
        encode('utf-8', $csv->string());
    } @$rows;
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
#sub decode_rows {
#    my ($self, $rows, $header) = @_;
#    my $res = [];
#    if ($header && 0 < scalar(@$header)) {
#    } else {
#        my $sample_row = $rows->[0];
#        my $decoded = $self->decode_row($sample_row);
#        $header = $decoded->{header};
#    }
#    push(@$res, $header);
#    map {
#        my $row = $_;
#        # my $decoded = $self->decode_row($row, $header);
#        my $decoded = $self->decode_row($row);
#        push(@$res, $decoded->{row});
#    } @$rows;
#    return $res;
#}

1;

