#
#   DB2.pm, engn_perldb2, db2_v71, 1.4 00/04/14 14:55:45
#
#   Copyright (c) 1995,1996,1997,1998,1999,2000  International Business Machines Corp.
#

{
    package DBD::DB2;

    use DBI;

    use DynaLoader;
    @ISA = qw(Exporter DynaLoader);

    @EXPORT_OK = qw( $attrib_dec
                     $attrib_int
                     $attrib_char
                     $attrib_float
                     $attrib_date
                     $attrib_ts
                     $attrib_binary
                     $attrib_blobfile
                     $attrib_clobfile
                     $attrib_dbclobfile );

    $VERSION = '0.73';
        require_version DBI 0.93;

    bootstrap DBD::DB2;

    use DBD::DB2::Constants;

    $err = 0;           # holds error code   for DBI::err
    $errstr = "";       # holds error string for DBI::errstr
    $drh = undef;       # holds driver handle once initialised

    $warn_success = $ENV{'WARNING_OK'};

    $attrib_dec = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_DECIMAL,
                    'Prec'    => 31,
                    'Scale'   => 4,
                  };
    $attrib_int = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_INTEGER,
                    'Prec'    => 10,
                    'Scale'   => 4,
                  };
    $attrib_char = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_CHAR,
                    'Prec'    => 0,
                  };
    $attrib_float = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_FLOAT,
                    'Prec'    => 15,
                    'Scale'   => 6,
                  };
    $attrib_date = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_DATE,
                    'Prec'    => 10,
                    'Scale'   => 9,
                  };
    $attrib_ts = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_TIMESTAMP,
                    'Prec'    => 26,
                    'Scale'   => 11,
                  };
    $attrib_binary = {
                    'ParamT'  => SQL_PARAM_INPUT_OUTPUT,
                    'Ctype'   => SQL_C_BINARY,
                    'Stype'   => SQL_BINARY,
                    'Prec'    => 0,
                  };
    $attrib_blobfile = {
                    'ParamT'  => SQL_PARAM_INPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_BLOB,
                    'File'    => 1,
                  };
    $attrib_clobfile = {
                    'ParamT'  => SQL_PARAM_INPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_CLOB,
                    'File'    => 1,
                  };
    $attrib_dbclobfile = {
                    'ParamT'  => SQL_PARAM_INPUT,
                    'Ctype'   => SQL_C_CHAR,
                    'Stype'   => SQL_DBCLOB,
                    'File'    => 1,
                  };

    sub driver{
        return $drh if $drh;
        my($class, $attr) = @_;

        $class .= "::dr";

        # not a 'my' since we use it above to prevent multiple drivers

        $drh = DBI::_new_drh($class, {
            'Name' => 'DB2',
            'Version' => $VERSION,
            'Err'    => \$DBD::DB2::err,
            'Errstr' => \$DBD::DB2::errstr,
            'Attribution' => 'DB2 DBD by IBM',
            });

        $drh;
    }

    1;
}


{   package DBD::DB2::dr; # ====== DRIVER ======
    use strict;

    sub errstr {
        DBD::DB2::errstr(@_);
    }

    sub connect {
        my($drh, $dbname, $user, $auth, $attr)= @_;

        # create a 'blank' dbh

        my $this = DBI::_new_dbh($drh, {
            'Name' => $dbname,
            'USER' => $user, 'CURRENT_USER' => $user
            });

        DBD::DB2::db::_login($this, $dbname, $user, $auth, $attr)
            or return undef;

        $this;
    }

}


{   package DBD::DB2::db; # ====== DATABASE ======
    use strict;

    sub errstr {
        DBD::DB2::errstr(@_);
    }

    sub prepare {
        my($dbh, $statement)= @_;

        # create a 'blank' dbh

        my $sth = DBI::_new_sth($dbh, {
            'Statement' => $statement,
            });

        DBD::DB2::st::_prepare($sth, $statement)
            or return undef;

        $sth;
    }

    sub tables {
        my ($dbh) = @_;
        my $tablesref = DBD::DB2::db::_tables($dbh);
        if( defined( $tablesref ) &&
            ref( $tablesref ) eq "ARRAY" )
        {
          return @$tablesref;
        }
        undef;
    }

    sub table_info {
        my ($dbh) = @_;
        my $sth = DBI::_new_sth($dbh, {});

        DBD::DB2::db::_table_info($dbh, $sth)
           or return undef;

        $sth;
    }
}


{   package DBD::DB2::st; # ====== STATEMENT ======
    use strict;

    sub errstr {
        DBD::DB2::errstr(@_);
    }

}

1;
