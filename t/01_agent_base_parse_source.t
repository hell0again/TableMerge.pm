use strict;
use utf8;
use Test::More 0.98;
use Test::Exception;

use TableMerge::Agent::Base;

{
    my ($obj, @args, @result, @expected);
    $obj = new TableMerge::Agent::Base;
    @args = ([
        "a,b,c",
        "1,2,3",
        "4,5,6"
    ]);
    @expected = ([
        [qw/a b c/],
        [qw/1 2 3/],
        [qw/4 5 6/],
    ]);
    (@result) = $obj->parse_source(@args);
    is_deeply(@result, @expected, "Got expected parse_source");
}
{
    my ($obj, @args, @result, @expected);
    $obj = new TableMerge::Agent::Base;
    @args = ([
        '"a" ,b,c',
        '1,2, 3',
        '4, "5" ,6'
    ]);
    @expected = ([
        [qw/a b c/],
        [qw/1 2 3/],
        [qw/4 5 6/],
    ]);
    (@result) = $obj->parse_source(@args);
    is_deeply(@result, @expected, "Got expected parse_source, allow whitespace surrounding the separation char");
}
{
    my ($obj, @args, @result, @expected);
    $obj = new TableMerge::Agent::Base;
    @args = ([
        '"a" ,b,"あ"',
        '1,2, "3\r\n4"',
        '4, "5\n7" ,6'
    ]);
    @expected = ([
        ["a", "b", "あ"],
        [qw/1 2 3\r\n4/],
        [qw/4 5\n7 6/],
    ]);
    (@result) = $obj->parse_source(@args);
    is_deeply(@result, @expected, "Got expected parse_source, containing line feeds");
}




{
    my ($obj, @args, @result, @expected);
    $obj = new TableMerge::Agent::Base;
    @args = ([
        '"a",b,c',
        '1,2,3,',
        '4,"5",6'
    ]);
    @expected = ([
        [qw/a b c/],
        [qw/1 2 3/],
        [qw/4 5 6/],
    ]);
    dies_ok {
        (@result) = $obj->parse_source(@args);
    } "Got expected parse_source, die with column num mismatch";
}
{
    my ($obj, @args, @result, @expected);
    $obj = new TableMerge::Agent::Base;
    @args = ([
        '"a",b,c',
        '1,2,"3',
        '4,"5",6'
    ]);
    @expected = ([
        [qw/a b c/],
        [qw/1 2 3/],
        [qw/4 5 6/],
    ]);
    dies_ok {
        (@result) = $obj->parse_source(@args);
    } "Got expected parse_source, die with malformed csv";
}

done_testing;





