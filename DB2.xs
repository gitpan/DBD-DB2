/*
    DB2.xs, engn_perldb2, db2_v5, 1.3 12/03/99 16:45:30
    Port DBD::DB2 driver 0.70 to OS390 system.
    Copyright (c) 1995,1996,1997,1998,1999 International Business Machines Corp.
*/

#include "DB2.h"


/* --- Variables --- */


DBISTATE_DECLARE;


MODULE = DBD::DB2	PACKAGE = DBD::DB2

PROTOTYPES: DISABLE

BOOT:
    items = 0;	/* avoid 'unused variable' warning */
    DBISTATE_INIT;
    /* XXX this interface will change: */
    DBI_IMP_SIZE("DBD::DB2::dr::imp_data_size", sizeof(imp_drh_t));
    DBI_IMP_SIZE("DBD::DB2::db::imp_data_size", sizeof(imp_dbh_t));
    DBI_IMP_SIZE("DBD::DB2::st::imp_data_size", sizeof(imp_sth_t));
    dbd_init(DBIS);


void
errstr(h)
    SV *	h
    CODE:
    /* called from DBI::var TIESCALAR code for $DBI::errstr	*/
    D_imp_xxh(h);
    ST(0) = sv_mortalcopy(DBIc_ERRSTR(imp_xxh));


MODULE = DBD::DB2	PACKAGE = DBD::DB2::dr

void
disconnect_all(drh)
    SV *	drh
    CODE:
    if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
	D_imp_drh(drh);
	sv_setiv(DBIc_ERR(imp_drh), (IV)1);
	sv_setpv(DBIc_ERRSTR(imp_drh),
		(char*)"disconnect_all not implemented");
	DBIh_EVENT2(drh, ERROR_event,
		DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	XSRETURN(0);
    }
    XST_mIV(0, 1);



MODULE = DBD::DB2    PACKAGE = DBD::DB2::db

void
_login(dbh, dbname, uid, pwd)
    SV *	dbh
    char *	dbname
    char *	uid
    char *	pwd
    CODE:
	D_imp_dbh(dbh);
    ST(0) = dbd_db_login(dbh, imp_dbh, dbname, uid, pwd) ? &sv_yes : &sv_no;


void
db_do(dbh, statement)
    SV *	dbh
    char *	statement
    CODE:
    I32 rows;
    /* XXX currently implemented as execute(prepare()) in DBI.pm	*/
    rows = dbd_db_do(dbh, statement);
    if (rows == 0)		/* ok with no rows affected 	*/
	XST_mPV(0,"0E0");	/* (true but zero)		*/
    else if (rows < -1)		/* -1 = unknown number of rows	*/
        XST_mUNDEF(0);		/* <= -2 means error		*/
    else
	XST_mIV(0,rows); 	/* typically 1, row count or -1	*/


void
commit(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
        warn("commit ineffective with AutoCommit enabled");
    ST(0) = dbd_db_commit(dbh,imp_dbh) ? &sv_yes : &sv_no;

void
rollback(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
        warn("rollback ineffective with AutoCommit enabled");
    ST(0) = dbd_db_rollback(dbh,imp_dbh) ? &sv_yes : &sv_no;


void
STORE(dbh, keysv, valuesv)
    SV *	dbh
    SV *	keysv
    SV *	valuesv
    CODE:
    ST(0) = &sv_yes;
    if (!dbd_db_STORE(dbh, keysv, valuesv))
	if (!DBIS->set_attr(dbh, keysv, valuesv))
	    ST(0) = &sv_no;

void
FETCH(dbh, keysv)
    SV *	dbh
    SV *	keysv
    CODE:
    SV *valuesv = dbd_db_FETCH(dbh, keysv);
    if (!valuesv)
	valuesv = DBIS->get_attr(dbh, keysv);
    ST(0) = valuesv;	/* dbd_db_FETCH did sv_2mortal	*/


void
disconnect(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    if ( !DBIc_ACTIVE(imp_dbh) ) {
        XSRETURN_YES;
    }
    /* pre-disconnect checks and tidy-ups */
    if (DBIc_CACHED_KIDS(imp_dbh)) {
        SvREFCNT_dec(DBIc_CACHED_KIDS(imp_dbh));
        DBIc_CACHED_KIDS(imp_dbh) = Nullhv;
    }
    /* Check for disconnect() being called whilst refs to cursors	*/
    /* still exist. This possibly needs some more thought.		*/
    if (DBIc_ACTIVE_KIDS(imp_dbh) && DBIc_WARN(imp_dbh) && !dirty) {

        char *plural = (DBIc_ACTIVE_KIDS(imp_dbh)==1) ? "" : "s";
        warn("disconnect(%s) invalidates %d active statement%s. %s",
             SvPV(dbh,na), (int)DBIc_ACTIVE_KIDS(imp_dbh), plural,
             "Either destroy statement handles or call finish on them before disconnecting.");
	
    }
    ST(0) = dbd_db_disconnect(dbh,imp_dbh) ? &sv_yes : &sv_no;


void
DESTROY(dbh)
    SV *	dbh
    CODE:
    D_imp_dbh(dbh);
    ST(0) = &sv_yes;
    if (!DBIc_IMPSET(imp_dbh)) {	/* was never fully set up	*/
        if (DBIc_WARN(imp_dbh) && !dirty && dbis->debug >= 2)
            warn("Database handle %s DESTROY ignored - never set up", SvPV(dbh,na));
    }
    else {
        /* pre-disconnect checks and tidy-ups */
	if (DBIc_CACHED_KIDS(imp_dbh)) {
	    SvREFCNT_dec(DBIc_CACHED_KIDS(imp_dbh));
	    DBIc_CACHED_KIDS(imp_dbh) = Nullhv;
	}
        if (DBIc_IADESTROY(imp_dbh)) {  /* want's ineffective destroy  */
            DBIc_ACTIVE_off(imp_dbh);
        }
        if (DBIc_ACTIVE(imp_dbh)) {
            if (DBIc_WARN(imp_dbh) && !dirty)
                warn("Database handle destroyed without explicit disconnect");
		
	    /* The application has not explicitly disconnected.  If AutoCommit	*/
	    /* is OFF, we will issue a rollback here.  If the application	*/
	    /* has explicitly committed the last transaction, the rollback	*/
	    /* will be harmless.						*/
	    if (!DBIc_has(imp_dbh, DBIcf_AutoCommit))
	        dbd_db_rollback(dbh, imp_dbh);
            dbd_db_disconnect(dbh,imp_dbh);
        }
        dbd_db_destroy(dbh);
    }



MODULE = DBD::DB2    PACKAGE = DBD::DB2::st


void
_prepare(sth, statement, attribs=Nullsv)
    SV *	sth
    char *	statement
    SV *	attribs
    CODE:
    DBD_ATTRIBS_CHECK("_prepare", sth, attribs);
    ST(0) = dbd_st_prepare(sth, statement, attribs) ? &sv_yes : &sv_no;


void
rows(sth)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    XST_mIV(0, dbd_st_rows(sth,imp_sth));


void
bind_param(sth, param, value, attribs=Nullsv)
    SV *	sth
    SV *	param
    SV *	value
    SV *	attribs
    CODE:
    DBD_ATTRIBS_CHECK("bind_param", sth, attribs);
    ST(0) = dbd_bind_ph(sth, param, value, attribs) ? &sv_yes : &sv_no;


void
set_conn_options(sth, opt, value)
	SV *	sth
	IV 		opt
	IV		value
	CODE:
	ST(0) = dbd_conn_opt(sth, opt, value) ? &sv_yes : &sv_no;
	
void
set_st_options(sth, opt, value)
	SV *	sth
	IV 		opt
	IV		value
	CODE:
	ST(0) = dbd_st_opts(sth, opt, value) ? &sv_yes : &sv_no;
	
void
execute(sth, ...)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    int retval;
    if (items > 1) {
	/* Handle binding supplied values to placeholders	*/
	int i, error = 0;
        SV *idx;
	if (items-1 != DBIc_NUM_PARAMS(imp_sth)) {
	    croak("execute called with %ld bind variables, %d needed",
		    items-1, DBIc_NUM_PARAMS(imp_sth));
	    XSRETURN_UNDEF;
	}
	idx = sv_2mortal(newSViv(0));
	for(i=1; i < items ; ++i) {
	    sv_setiv(idx, i);
	    if (!dbd_bind_ph(sth, idx, ST(i), Nullsv))
		++error;
	}
	if (error) {
	    XSRETURN_UNDEF;	/* dbd_bind_ph already registered error	*/
	}
    }
    retval = dbd_st_execute(sth);
    if (retval == 0)		/* ok with no rows affected 	*/
	XST_mPV(0,"0E0");	/* (true but zero)		*/
    else if (retval < -1)       /* -1 = unknown number of rows	*/
        XST_mUNDEF(0);		/* <= -2 means error		*/
    else
	XST_mIV(0,retval); 	/* typically 1, row count or -1	*/


void
fetch(sth)
    SV *	sth
    CODE:
    AV *    av = dbd_st_fetch(sth);

	ST(0) = (av) ? sv_2mortal(newRV((SV *)av)) : &sv_undef;

void
fetchrow(sth)
    SV *	sth
    PPCODE:
    D_imp_sth(sth);
    AV *av;
    if (GIMME == G_SCALAR && DBIc_COMPAT(imp_sth)) {	/* XXX Oraperl	*/
	/* This non-standard behaviour added only to increase the	*/
	/* performance of the oraperl emulation layer (Oraperl.pm)	*/
	XSRETURN_IV(DBIc_NUM_FIELDS(imp_sth));
    }
    av = dbd_st_fetch(sth);
    if (av) {
	int num_fields = AvFILL(av)+1;
	int i;
	EXTEND(sp, num_fields);
	for(i=0; i < num_fields; ++i) {
	    PUSHs(AvARRAY(av)[i]);
	}
    }



void
blob_read(sth, field, offset, len, destrv=Nullsv, destoffset=0)
    SV *	sth
    int	field
    long	offset
    long	len
    SV *	destrv
    long	destoffset
    CODE:
    if (!destrv)
	destrv = sv_2mortal(newRV(newSV(0)));
    if (dbd_st_blob_read(sth, field, offset, len, destrv, destoffset))
	ST(0) = SvRV(destrv);
    else
	ST(0) = &sv_undef;


void
STORE(sth, keysv, valuesv)
    SV *	sth
    SV *	keysv
    SV *	valuesv
    CODE:
    ST(0) = &sv_yes;
    if (!dbd_st_STORE(sth, keysv, valuesv))
	if (!DBIS->set_attr(sth, keysv, valuesv))
	    ST(0) = &sv_no;


void
FETCH(sth, keysv)
    SV *	sth
    SV *	keysv
    CODE:
    SV *valuesv = dbd_st_FETCH(sth, keysv);
    if (!valuesv)
	valuesv = DBIS->get_attr(sth, keysv);
    ST(0) = valuesv;	/* dbd_st_FETCH did sv_2mortal	*/


void
finish(sth)
    SV *	sth
    CODE:
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    if (!DBIc_ACTIVE(imp_dbh)) {
	/* Either an explicit disconnect() or global destruction	*/
	/* has disconnected us from the database. Finish is meaningless	*/
	/* XXX warn */
	XSRETURN_YES;
    }
    if (!DBIc_ACTIVE(imp_sth)) {
	/* No active statement to finish	*/
	XSRETURN_YES;
    }
    ST(0) = dbd_st_finish(sth,imp_sth) ? &sv_yes : &sv_no;


void
DESTROY(sth)
    SV *	sth
    PPCODE:
    D_imp_sth(sth);
    ST(0) = &sv_yes;
    if (!DBIc_IMPSET(imp_sth)) {	/* was never fully set up	*/
        if (DBIc_WARN(imp_sth) && !dirty && dbis->debug >=2)
            warn("Statement handle %s DESTROY ignored - never set up", SvPV(sth,na));
    }
    else {
        if (DBIc_IADESTROY(imp_sth)) {  /* want's ineffective destroy   */
            DBIc_ACTIVE_off(imp_sth);
        }
        if (DBIc_ACTIVE(imp_sth))
            dbd_st_finish(sth,imp_sth);
        dbd_st_destroy(sth);
    }



# end of DB2.xs
