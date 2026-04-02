#!/usr/perl -w
use strict;

##负责从function map中查找对应的注释文件
#sub do_help
#{
#	my $need_output = 0;
#	my $function_name = $_[0];
#  while (my $line = <STDIN>)
#	{
#		chomp($line);
#		if ($line =~ /$function_name/) {
#			$need_output = 1;
#		}
#		if ($need_output > 0) 
#		{
#			if ($line =~ /SETP/) 
#			{
#				$need_output = 0;
#			} 
#			else 
#			{
#				print STDOUT $line.'\n';
#
#			}
#		}	
#	} 
#}


MAIN:
{
#STEP_2.2:check_bootstrap_rs_list
#读入文件，抽取出函数名，然后去日志文件中grep该函数的执行结果；
  my $need_output = 0;
  while (my $line = <STDIN>)
  {
    chomp($line);
    my $function_name;
    if ($line =~ /^STEP_(\d+)\.(\d+):(.+)/)
    {
      my $log = $ARGV[0];
      $function_name = $3; 
      my $grepout = `grep "$function_name execute success" $log/* -r`;
      if (!$grepout) {
        print STDOUT "bootstarp failed at ".$function_name."\n";
        $need_output = 1;
      }
    }
    if ($need_output > 0) 
    {
      print STDOUT $line."\n";
      next;
      if ($line =~ /^STEP_/) 
      {
        #print STDOUT $line."\n";
        $need_output = 0;
      } 
    }
  }
}
