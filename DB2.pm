#
#   engn/perldb2/DB2.pm, engn_perldb2, db2_v81, 1.6 00/09/06 15:57:59
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

    $VERSION = '0.74';
        require_version DBI 0.93;

    bootstrap DBD::DB2;

    use DBD::DB2::Constants;

    $err = 0;           # holds error code   for DBI::err
    $errstr = "";       # holds error string for DBI::errstr
    $drh = undef;       # holds driver handle once initialised

    $warn_success = $ENV{'WARNING_OK'};

    $attrib_dec = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_DECIMAL,
                    'PRECISION'      => 31,
                    'SCALE'          => 4,
                  };
    $attrib_int = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_INTEGER,
                    'PRECISION'      => 10,
                  };
    $attrib_char = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_CHAR,
                    'PRECISION'      => 0,
                  };
    $attrib_float = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_FLOAT,
                    'PRECISION'      => 15,
                    'SCALE'          => 6,
                  };
    $attrib_date = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_DATE,
                    'PRECISION'      => 10,
                  };
    $attrib_ts = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_TIMESTAMP,
                    'PRECISION'      => 26,
                    'SCALE'          => 11,
                  };
    $attrib_binary = {
                    'db2_param_type' => SQL_PARAM_INPUT_OUTPUT,
                    'db2_c_type'     => SQL_C_BINARY,
                    'db2_type'       => SQL_BINARY,
                    'PRECISION'      => 0,
                  };
    $attrib_blobfile = {
                    'db2_param_type' => SQL_PARAM_INPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_BLOB,
                    'db2_file'       => 1,
                  };
    $attrib_clobfile = {
                    'db2_param_type' => SQL_PARAM_INPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_CLOB,
                    'db2_file'       => 1,
                  };
    $attrib_dbclobfile = {
                    'db2_param_type' => SQL_PARAM_INPUT,
                    'db2_c_type'     => SQL_C_CHAR,
                    'db2_type'       => SQL_DBCLOB,
                    'db2_file'       => 1,
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

    sub do {
        my($dbh, $statement, $attr, @params) = @_;
        my $rows = 0;

        if( -1 == $#params )
        {
          # No parameters, use execute immediate
          $rows = DBD::DB2::db::_do( $dbh, $statement );
          if( 0 == $rows )
          {
            $rows = "0E0";
          }
          elsif( $rows < -1 )
          {
            undef $rows;
          }
        }
        else
        {
          $rows = $dbh->SUPER::do( $statement, $attr, @params );
        }
        return $rows
    }

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
        my $tablesref = DBD::DB2::db::_tables( $dbh );
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

        DBD::DB2::st::_table_info( $sth )
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
