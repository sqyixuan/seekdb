#!/usr/bin/env perl
# create date: 23 Mar 2016
# description:
use strict;
use warnings;
use Data::Dumper;
use Getopt::Std;
our ($opt_h, $opt_v, $opt_f);

exit main(@ARGV);
################################################################
package main;
sub print_help
{
    my $msg =<< 'HELP_MSG';
    Usage: program [options]
    Options:
HELP_MSG

    print "$msg\n";
}

sub print_version
{
}

sub main
{
    if (!getopts('thf:')) {
	return 1;
    } elsif ($opt_h) {
	print_help();
    } elsif ($opt_v) {
	print_version();
    } else {
	$opt_f = 100 unless defined $opt_f;
	while(<>) {
	    if ($_ =~ /pair_event\(ev=id:(\d+)\|.+\)/) {
		my $id = $1;
		print "================pair_events(id:${id})================\n";
		my @events = split /\|/, $_;
		my @tosort;
		for my $ev (@events)
		{
		    #print $ev, "\n";
		    if ($ev =~ m/name:(\w+)\stotal_time:(\d+)\scount:(\d+)\savg:(\d+)/)
		    {
			#print "$1 $2 $3 $4\n";
			push @tosort, [$1, $2, $3, $4];
		    }
		}
		map {print $_->[0], " count:", $_->[2], " avg_us:", $_->[3], "\n" unless ($_->[2] <= $opt_f);} sort {$b->[3] <=> $a->[3]} @tosort;
	    } elsif ($_ =~ /event\(ev=id:(\d+)\|.+\)/) {
		my $id = $1;
		print "================events(id:${id})================\n";
		my @events = split /\|/, $_;
		my @tosort;
		for my $ev (@events)
		{
		    #print $ev, "\n";
		    if ($ev =~ m/name:(\w+)\stype:(\d+)\stotal_time:(\d+)\scount:(\d+)\savg:(\d+)/)
		    {
			#print "$1 $2 $3 $4\n";
			push @tosort, [$1, $2, $3, $4, $5];
		    }
		}
		map {print $_->[0], "(", $_->[1], ") count:", $_->[3], " avg_us:", $_->[4], "\n" unless ($_->[3] <= $opt_f);} sort {$b->[4] <=> $a->[4]} @tosort;
	    }
	}
    }
    return 0;
}
