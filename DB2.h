/*
   $Id: DB2.h,v 0.9 1998/03/16 20:36:56 mhm Rel $

   Copyright (c) 1995  Mike Moran

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README
   file.

*/

#define NEED_DBIXS_VERSION 7

#include <sqlcli.h>
#include <sqlcli1.h>
#include <sqlext.h>

#include <DBIXS.h>		/* installed by the DBI module	*/

/* read in our implementation details */

#include "dbdimp.h"

void dbd_init _((dbistate_t *dbistate));

int  dbd_db_login _((SV *dbh, imp_dbh_t *imp_dbh, char *dbname, char *uid, char *pwd));
int  dbd_db_do _((SV *sv, char *statement));
int  dbd_db_commit _((SV *dbh, imp_dbh_t *imp_dbh));
int  dbd_db_rollback _((SV *dbh, imp_dbh_t *imp_dbh));
int  dbd_db_disconnect _((SV *dbh, imp_dbh_t *imp_dbh));
void dbd_db_destroy _((SV *dbh));
int  dbd_db_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_db_FETCH _((SV *dbh, SV *keysv));


int  dbd_st_prepare _((SV *sth, char *statement, SV *attribs));
int  dbd_st_rows    _((SV *sth, imp_sth_t *imp_sth));
int  dbd_bind_ph _((SV *h, SV *param, SV *value, SV *attribs));
int  dbd_st_execute _((SV *sv));
AV  *dbd_st_fetch _((SV *sv));
int  dbd_st_finish _((SV *sth, imp_sth_t *imp_sth));
int	 dbd_st_destroy _((SV *sth));
int  dbd_st_blob_read _((SV *sth, int field, long offset, long len,
			SV *destrv, long destoffset));
int  dbd_st_STORE _((SV *dbh, SV *keysv, SV *valuesv));
SV  *dbd_st_FETCH _((SV *dbh, SV *keysv));


/* end of DB2.h */
