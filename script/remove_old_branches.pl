#!/usr/bin/env perl
use strict;
use warnings;
use autodie;
use Getopt::Std;
use DateTime::Format::ISO8601;
use DateTime::Duration;

our($opt_V, $opt_h);
exit main(@ARGV);
################
package main;
our $VERSION = '0.1';

sub get_last_update_time
{
    my $last_update_time;
    my $branch = shift;
    my $last_log = `git log -n1 --date=iso-strict $branch`;
    if ($last_log =~ /Date:\s+(.*)/g) {
	$last_update_time = $1;
	my $iso8601 = DateTime::Format::ISO8601->new;
	my $dt = $iso8601->parse_datetime( $last_update_time );
	$last_update_time = $dt;
    }
    return $last_update_time;
}

sub is_old_branch
{
    my $dt = shift;
    my $duration = DateTime::Duration->new( months => 6);
    my $now = DateTime->from_epoch( epoch => time() );
    return ($dt + $duration < $now);
}

sub main
{
    getopts('hV');
    system("git remote prune --dry-run origin");
    my $all_remote_branches = `git branch -l --remote`;
    my @all_branches = split '\n', $all_remote_branches;
    for my $br (@all_branches) {
	my $dt = get_last_update_time($br);
	my $too_old = is_old_branch($dt);
	print "Branch:$br\nLastModifiedAt: $dt\nTooOld: ${too_old}\n\n";
	if ($br =~ /release/) {
	    print "Skip release branch\n\n";
	    next;
	} elsif ($too_old) {
	    $|++;                   # autoflush STDOUT
	    print "Delete this branch $br (y/n) ?";
	    while(<STDIN>)
	    {
		if (/y/) {
		    print "Deleting branch $br\n";
		    my ($repo, $ref) = split /\//, $br;
		    system("git push --delete $repo $ref\n");
		    last;
		} elsif (/n/) {
		    last;
		}
		print "Delete this branch $br (y/n)?";
	    }
	}
    }
    return 0;
}
