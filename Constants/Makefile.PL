#
#	Copyright (c) 1995,1996,1998 International Business Machines Corp.
#
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
%opts = (    'NAME'      => 'DBD::DB2::Constants',
    'VERSION'   => '0.13',
    'LIBS'      => [''],   # e.g., '-lm'
    'DEFINE'    => '',     # e.g., '-DHAVE_SOMETHING'
);

$DB2 = $ENV{DB2_HOME} || "/usr/lpp/db2_05_00";

$OCIINCLUDE = $MK{INCLUDE} || "-I$DB2/include";

$opts{INC} = "$OCIINCLUDE";

# --- Get UNIX name
chop($uname = `uname`);

# --- Handle special cases
if ($uname eq "HP-UX")
{
  $opts{DEFINE} .= ' +Onolimit +e';
}

WriteMakefile( %opts );
 
sub MY::dist {          # new name with 5.001e
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
	cd ..; compress $(DISTNAME)-$(VERSION).tar;
	@chmod -w ../$(DISTNAME)-$(VERSION).tar.Z
	mv ../$(DISTNAME)-$(VERSION).tar.Z /u/ftp/pub/perl/db/DBD/DB2
	@cp README /u/ftp/pub/perl/db/DBD/DB2/
	@cd /u/ftp/pub/perl/db/DBD/DB2; pwd; ls -l *.Z
';
}
