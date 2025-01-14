#!/usr/bin/perl

#
# A script to find one monogoDB record and print it nicely
#
# Usage: find_one.pl <db> <collection> "field=value"
#

use strict;
use warnings;
use MongoDB;
use Data::Dumper;
use Carp;

use FindBin;
use lib "$FindBin::Bin/../lib";
use GEO;

use Options;

BEGIN: {
  Options::use(qw(d q v h fuse=i db_name=s full));
    Options::useDefaults(fuse => -1, db_name=>'geo');
    Options::get();
    die Options::usage() if $options{h};
    $ENV{DEBUG} = 1 if $options{d};
    GEO->db_name($options{db_name});
}

sub main {
    my $usage="$0 <geo_id>\n";
    my $geo_id=shift or die $usage;
    my $geo=GEO->factory($geo_id);
    print Dumper($geo) if $ENV{DEBUG};
    
    if ($geo->can('report')) {
	print "\nReport:\n";
	my $report=$geo->report(full=>$options{full});
	print "$report\n";
#	print Dumper($report);
    }
}

main(@ARGV);
