use strict;
use warnings;
use t::Utils;
use TheSchwartz::Simple;

plan tests => 10;

run_test {
    my $dbh = shift;
    my $sch = TheSchwartz::Simple->new($dbh);

    $sch->insert('fetch', 'http://wassr.jp/');
    $sch->insert(
        TheSchwartz::Simple::Job->new(
            funcname => 'fetch',
            arg      => 'http://pathtraq.com/',
            priority => 3,
        )
    );

    my $sth = $dbh->prepare('SELECT jobid, funcid, arg, priority FROM job ORDER BY jobid ASC');
    $sth->execute;

    my $row = $sth->fetchrow_hashref;
    ok $row;
    is $row->{jobid},    1;
    is $row->{funcid},   $sch->funcname_to_id( $dbh, 'fetch' );
    is $row->{arg},      'http://wassr.jp/';
    is $row->{priority}, undef;

    my $row = $sth->fetchrow_hashref;
    ok $row;
    is $row->{jobid},    2;
    is $row->{funcid},   $sch->funcname_to_id( $dbh, 'fetch' );
    is $row->{arg},      'http://pathtraq.com/';
    is $row->{priority}, 3;
};

