/*
   engn/perldb2/DB2.h, engn_perldb2, db2_v81, 1.6 01/03/29 17:06:43

   Copyright (c) 1995,1996,1997,1998,1999,2000  International Business Machines Corp.
*/

#define NEED_DBIXS_VERSION 7

#include <sqlcli.h>
#include <sqlcli1.h>
#include <sqlext.h>
#include <sqlstate.h>

#include <DBIXS.h>              /* installed by the DBI module  */

/* read in our implementation details */

#include "dbdimp.h"

#include <dbd_xsh.h>            /* installed by the DBI module  */

int  dbd_db_login2 _((SV *dbh, imp_dbh_t *imp_dbh, char *dbname, char *uid, char *pwd, SV *attr));
int  dbd_db_ping _((SV *dbh));
AV*  dbd_db_tables _((SV *dbh, imp_dbh_t *imp_dbh));
int  dbd_st_table_info _((SV *sth, imp_sth_t *imp_sth));

/* end of DB2.h */
