#
#   engn/perldb2/Constants/Makefile.PL, engn_perldb2, db2_v6, 1.3 99/01/14 10:59:42
#
#   Copyright (c) 1995,1996,1998,1999 International Business Machines Corp.
#
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
%opts = (    
	'NAME'		=> 'DBD::DB2::Constants',
	'VERSION_FROM'	=> 'Constants.pm',
	'LIBS'		=> [''],   # e.g., '-lm'
	'DEFINE'	=> '',     # e.g., '-DHAVE_SOMETHING'
);

# --- Operating system
my $os = $^O;

# --- Where is DB2 installed...

my $DB2 = $ENV{'DB2_HOME'};
$DB2 =~ s:\\:/:g	if $os eq 'MSWin32';
die "DB2_HOME environment variable must be set.\n" unless $DB2;
die "DB2_HOME environment variable ($DB2) not valid.\n" unless -d $DB2;

$DB2INCLUDE = $MK{INCLUDE} || "-I$DB2/include";

$opts{INC} = "$DB2INCLUDE";

# --- Handle special cases
if ($os eq "hpux")
{
  $opts{DEFINE} .= ' +Onolimit +e';
}
elsif ($os eq "aix")
{
  $opts{DEFINE} .= ' -qcpluscmt';
}
elsif ($os eq "solaris")
{
  $opts{DEFINE} .= ' -xCC';
}

WriteMakefile( %opts );
