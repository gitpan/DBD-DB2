/*
   %W%, %I% %E% %U%

   Copyright (c) 1995,1996,1997,1998,1999,2000,2001  International Business Machines Corp.
*/

#define NEED_DBIXS_VERSION 7

#include <sqlcli.h>
#ifndef AS400
#include <sqlcli1.h>
#include <sqlext.h>
#endif
#include <sqlstate.h>

#include <DBIXS.h>              /* installed by the DBI module  */

/* read in our implementation details */

#include "dbdimp.h"

#include <dbd_xsh.h>            /* installed by the DBI module  */
#ifndef AS400
AV*  dbd_data_sources _((SV *drh));
#endif
int  dbd_db_login2 _((SV *dbh, imp_dbh_t *imp_dbh, char *dbname, char *uid, char *pwd, SV *attr));
int  dbd_db_ping _((SV *dbh));
int  dbd_st_table_info _((SV *sth, imp_sth_t *imp_sth, SV *attr));

/* end of DB2.h */
