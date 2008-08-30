use strict;
use warnings;
use t::Utils;
use TheSchwartz::Simple;

plan tests => 5;

run_test {
    my $dbh = shift;
    my $sch = TheSchwartz::Simple->new($dbh);
    isa_ok $sch, 'TheSchwartz::Simple';
    is $sch->funcname_to_id($dbh, 'foo'), 1;
    is $sch->funcname_to_id($dbh, 'bar'), 2;
    is $sch->funcname_to_id($dbh, 'foo'), 1;
    is $sch->funcname_to_id($dbh, 'baz'), 3;
};

