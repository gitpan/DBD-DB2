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

print "\nConfiguring DBD::DB2 version $opts{VERSION_FROM}...\n";
print "Remember to actually read the CAVEATS file!\n\n";

# --- Where is DB2 installed...

my $DB2 = $ENV{DB2_HOME} || '/usr/lpp/db2_05_00';

print "Using DB2 in $DB2\n";


# --- What DB2 is installed...


# read list of libs that DB2 requires
$sysliblist ="-L$DB2/lib -ldb2";
print "DB2 sysliblist: $sysliblist\n";


$OCIINCLUDE = $MK{INCLUDE} || "-I$DB2/include";

$opts{LIBS} = [ $sysliblist ];
$opts{INC}  = "$OCIINCLUDE -I$Config{sitearch}/auto/DBI";
#$opts{dynamic_lib} = { OTHERLDFLAGS => '$(COMPOBJS) '.$linkwith };
$opts{dynamic_lib} = { OTHERLDFLAGS => '$(COMPOBJS) '};
$opts{OBJECT} = '$(O_FILES)';
$opts{DEFINE} = '';
# --- Handle special cases ---



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

sub ask {
    my($prompt, $default) = @_;
	printflush(STDOUT,"$prompt: ");
	printflush(STDOUT,"[$default] ") if $default;
    my $value = <>;
	chomp $value;
	return $value or $default;
}


# ---------------------------------------------------------------


sub MY::dist {		# new name with 5.001e
    '
DISTFILES=RCS/*

ci:
	-ci -u $(DISTFILES) 2>&1 | grep -v "no lock set by "
	-rcs -q -Nv`echo "$(VERSION)" | tr . _`: $(DISTFILES)

rcsdiff:
	for f in $(DISTFILES); do rcsdiff -r$(V) $$f; done 2>&1 | less

dist:  ci clean
	rcs -q -sRel $(DISTFILES)
	co -q -u     $(DISTFILES)
	rm -f Makefile *~ t/*~
	cd ..; tar cvFf $(DISTNAME)-$(VERSION).tar \
		`sed -e "s:^:$(BASEEXT)/:" < $(BASEEXT)/MANIFEST`
	cd ..; gzip -9 $(DISTNAME)-$(VERSION).tar;
	@chmod -w ../$(DISTNAME)-$(VERSION).tar.gz
	mv ../$(DISTNAME)-$(VERSION).tar.gz /usr/misc/ftp/pub/perl/db/DBD/DB2/
	@cp README /usr/misc/ftp/pub/perl/db/DBD/DB2/
	@cd /usr/misc/ftp/pub/perl/db/DBD/DB2; pwd; ls -l *.gz
';
}

*{"MY::distclean"} = \&MY::dist;	# remain compatible with 5.001

__END__
