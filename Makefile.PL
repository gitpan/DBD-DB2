#
#   engn/perldb2/Makefile.PL, engn_perldb2, db2_v6, 1.3 99/01/12 13:48:04
#
#   Copyright (c) 1995,1996,1998,1999 International Business Machines Corp.
#
#!/usr/local/bin/perl -sw

use ExtUtils::MakeMaker qw(&WriteMakefile $Verbose );
use Getopt::Std;
use Config;
require 'flush.pl';

use DBI;	# The DBI must be installed before we can build a DBD

%opts = (
    'NAME'    => 'DBD::DB2',
    'VERSION_FROM' => 'DB2.pm',
);

# --- Introduction

print "\nConfiguring DBD::DB2...\n";
print "Remember to actually read the README and CAVEATS files!\n\n";

# --- Operating system
my $os = $^O;

# --- Where is DB2 installed...

my $DB2 = $ENV{'DB2_HOME'};
$DB2 =~ s:\\:/:g	if $os eq 'MSWin32';
die "DB2_HOME environment variable must be set to installed location of DB2.\n" 
    unless $DB2;
die "DB2_HOME environment variable ($DB2) not valid.\n" unless -d $DB2;

print "Using DB2 in $DB2\n";

$DB2INCLUDE = $MK{INCLUDE} || "-I$DB2/include";

$opts{INC}  = "$DB2INCLUDE -I$Config{sitearch}/auto/DBI";
$opts{dynamic_lib} = { OTHERLDFLAGS => '$(COMPOBJS) '};
$opts{OBJECT} = '$(O_FILES)';
$opts{DEFINE} = '';

# libraries required to build DBD::DB2 driver
if ($os eq 'MSWin32')
{
  $sysliblist = "-L$DB2/lib db2cli";
}
else
{
  $sysliblist = "-L$DB2/lib -ldb2";
}

# --- Handle special cases ---
if ($os eq 'hpux')
{
  $sysliblist .= ' -lcl';
  $opts{DEFINE} .= ' +e';
}

print "DB2 sysliblist: $sysliblist\n";
$opts{LIBS} = [ $sysliblist ];

# log key platform information to help me help you quickly
print "System: perl$] @Config{qw(myuname archname dlsrc)}\n";
print "Compiler: @Config{qw(cc optimize ccflags)}\n";
print "  CC:       $MK{CC}\n"		if $MK{CC};
print "  CFLAGS:   $MK{CFLAGS}\n"	if $MK{CFLAGS};
print "  LDFLAGS:  $MK{LDFLAGS}\n"	if $MK{LDFLAGS};
print "  LDSTRING: $MK{LDSTRING}\n"	if $MK{LDSTRING};

print "\n";

WriteMakefile(%opts);

exit 0;


sub MY::post_initialize{
'
default_target: all
';
}

__END__
