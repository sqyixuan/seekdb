#!/bin/env perl
use strict;
use warnings;

my %total_map;

while(<>)
{
    my $line = $_;
    #print $line, "\n";
    while ($line =~ m/\| \[([^\]\s]+)\] ([^|(\[\]]+)(?:[^|]*)\su=(\d+)/g) {
	#print "[$1] $2 => $3\n";
	if ($2 =~ /query=/) {next;}
	my $key = "[$1] $2";
	if (defined $total_map{$key}) {
	    ${$total_map{$key}}[0]++;
	    ${$total_map{$key}}[1] += $3;
	} else {
	    $total_map{$key} = [];
	    ${$total_map{$key}}[0] = 1;
	    ${$total_map{$key}}[1] = $3;
	}
    }
}

map {print "avg=", $_->[2], " count=", $_->[1], " " ,$_->[0], "\n";} sort {$b->[2] <=> $a->[2]} map {[$_, ${$total_map{$_}}[0], ${$total_map{$_}}[1]/${$total_map{$_}}[0]];} keys %total_map;
