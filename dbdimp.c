/*
   $Id: dbdimp.c,v 0.12 1996/10/07 18:00:05 mhm Rel $

   Copyright (c) 1995,1996 International Business Machines Corp.

*/

#include <stdio.h>
#include "DB2.h"

#define EOI(x)  if (x < 0 || x == 100) return (0)
#define NHENV   SQL_NULL_HENV
#define NHDBC   SQL_NULL_HDBC
#define NHSTMT  SQL_NULL_HDBC
#define ERRTYPE(x)	((x < 0) && (x == SQL_ERROR))


SQLHENV henv;

DBISTATE_DECLARE;

void
dbd_init(dbistate)
    dbistate_t *dbistate;
{
    DBIS = dbistate;
}

SQLINTEGER
check_error(h, rc, h_env, h_conn, h_stmt,what)
SV *h;
IV  rc, h_env, h_conn, h_stmt;
SQLCHAR  *what;
{
    if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
        if (h_env == NHENV) {
            do_error(h,rc,NHENV,NHDBC,NHSTMT,what);
        } else {
			if (rc == SQL_SUCCESS_WITH_INFO) {
				if (dbis->debug > 2) {
            		do_error(h,rc,h_env,h_conn,h_stmt,what);
				}
			} else {
				do_error(h,rc,h_env,h_conn,h_stmt,what);
			}
        }
    }
    return((rc == SQL_SUCCESS_WITH_INFO ? SQL_SUCCESS : rc));
}

void
do_error(SV *h, SQLINTEGER rc, SQLHENV h_env, SQLHDBC h_conn, SQLHSTMT h_stmt, 
	 SQLCHAR  *what)
{
    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);
    SQLSMALLINT length;
	SV *state = DBIc_STATE(imp_xxh);
    SQLINTEGER  sqlcode;
	SQLCHAR  sqlstate[6];
    SQLCHAR  msg[SQL_MAX_MESSAGE_LENGTH+1];
    SQLINTEGER msgsize = SQL_MAX_MESSAGE_LENGTH+1;

    msg[0]='\0';
    if (h_env != NHENV) {
        SQLError(h_env,h_conn,h_stmt, sqlstate, &sqlcode, msg,
                 msgsize,&length);
    } else {
        strcpy((SQLCHAR  *)msg, (char *)what);
	}
    sv_setiv(DBIc_ERR(imp_xxh), (IV)sqlcode);
    sv_setpv(errstr, (SQLCHAR  *)msg);
	sv_setpv(state,(SQLCHAR  *)sqlstate);
    if (what && (h_env == NHENV)) {
        sv_catpv(errstr, " (DBD: ");
        sv_catpv(errstr, (SQLCHAR  *)what);
        sv_catpv(errstr, ")");
    }
    DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);
    if (dbis->debug >= 2)
    fprintf(DBILOGFP, "%s error %d recorded: %s\n",
        what, rc, SvPV(errstr,na));
}

void
fbh_dump(fbh, i)
    imp_fbh_t *fbh;
    SQLINTEGER i;
{
    FILE *fp = DBILOGFP;
    fprintf(fp, "fbh %d: '%s' %s, ",
		i, fbh->cbuf, (fbh->nullok) ? "NULLable" : "");
    fprintf(fp, "type %d,  dbsize %ld, dsize %ld, p%d s%d\n",
	    fbh->dbtype, (long)fbh->dbsize, (long)fbh->dsize,
	    fbh->prec, fbh->scale);
    fprintf(fp, "   out: ftype %d, indp %d, bufl %d, rlen %d, rcode %d\n",
	    fbh->ftype, fbh->indp, fbh->bufl, fbh->rlen, fbh->rcode);
}

static SQLINTEGER
dbtype_is_long(dbtype)
    SQLINTEGER dbtype;
{
    /* Is it a LONG, LONG RAW, LONG VARCHAR or LONG VARRAW type?	*/
    /* Return preferred type code to use if it's a long, else 0.	*/
    if (dbtype == SQL_CLOB || dbtype == SQL_BLOB)	/* LONG or LONG RAW		*/
	return dbtype;			/*		--> same	*/
    if (dbtype == SQL_LONGVARCHAR)			/* LONG VARCHAR			*/
	return SQL_CLOB;			/*		--> LONG	*/
    if (dbtype == SQL_LONGVARBINARY)			/* LONG VARRAW			*/
	return SQL_BLOB;			/*		--> LONG RAW	*/
    return 0;
}

static SQLINTEGER
dbtype_is_string(dbtype)	/* 'can we use SvPV to pass buffer?'	*/
    SQLINTEGER dbtype;
{
    switch(dbtype) {
    	case SQL_VARCHAR:		/* VARCHAR2	*/
    	case SQL_INTEGER:			/* LONG		*/
    	case SQL_CHAR:			/* RAW		*/
    	case SQL_LONGVARBINARY:	/* LONG RAW	*/
    	case SQL_LONGVARCHAR:	/* LONG VARCHAR*/
    	case SQL_CLOB:			/* Char blob */
		return 1;
    }
    return 0;
}


/* ================================================================== */


/* ================================================================== */

SQLINTEGER
dbd_db_connect(dbh,henv,dbname,uid,pwd)
    SV *dbh;
    IV  henv;
    SQLCHAR  *dbname,*uid,*pwd;
{
    D_imp_dbh(dbh);
    SQLCHAR  *msg;
    SQLINTEGER ret;

    ret = SQLAllocConnect(henv,&imp_dbh->hdbc);
    msg = (ERRTYPE(ret) ? "Connect allocation failed" :
                                "Invalid Handle");
    ret = check_error(dbh,ret,henv,NHDBC,NHSTMT,msg);
   	if (ret != SQL_SUCCESS) { 
		return(ret);			/* Must return SQL codes not perl/DBD/DBI */
	}						 	/* otherwise failure is not caught  */
								
    ret = SQLConnect(imp_dbh->hdbc,dbname,SQL_NTS,uid,SQL_NTS,pwd,SQL_NTS);
    msg = ( ERRTYPE(ret) ? "Connect failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
   	if (ret != SQL_SUCCESS) {	
		return(ret);			/* Must return SQL codes not perl/DBD/DBI */
	} 							/* otherwise failure is not caught  */
    return 1;
}

SQLINTEGER
dbd_db_login(dbh, dbname, uid, pwd)
    SV *dbh;
    SQLCHAR  *dbname;
    SQLCHAR  *uid;
    SQLCHAR  *pwd;
{
    D_imp_dbh(dbh);
    SQLINTEGER ret;
    SQLCHAR  *msg;

    if (henv == NHENV) {
        ret = SQLAllocEnv(&henv);
        msg = (henv == NHENV ?
                "Total Environment allocation failure!" :
                "Environment allocation failed");
        ret = check_error(dbh,ret,henv,NHDBC,NHSTMT,msg);
        EOI(ret);
    } else {
        msg = "Current limitation is ONE connection per process";
        ret = check_error(dbh,~SQL_SUCCESS,NHENV,NHDBC, NHSTMT,msg);
        return(0);
    }
    ret = dbd_db_connect(dbh,henv,dbname, uid, pwd);
    EOI(ret);

    DBIc_IMPSET_on(imp_dbh);	/* imp_dbh set up now			*/
    DBIc_ACTIVE_on(imp_dbh);	/* call disconnect before freeing	*/
    return 1;
}


SQLINTEGER
dbd_db_do(dbh, statement)	/* return 1 else 0 on failure		*/
    SV *dbh;
    SQLCHAR  *statement;
{
	D_imp_dbh(dbh);
    SQLINTEGER ret;
    SQLCHAR  *msg;
	SQLHSTMT stmt;

    ret = SQLAllocStmt(imp_dbh->hdbc,&stmt);
    msg = "Statement allocation error";
    ret = check_error(NULL,ret,henv,imp_dbh->hdbc,NHSTMT,msg);

    EOI(ret);

    ret = SQLExecDirect(stmt,statement,SQL_NTS);
    ret = check_error(NULL,ret,henv,imp_dbh->hdbc,stmt,
                    "Execute immediate failed");
    (void)check_error(NULL, SQLFreeStmt(stmt, SQL_DROP),
    		henv,imp_dbh->hdbc,NHSTMT, "Statement destruction error");
    EOI(ret);

    return 1;
}


SQLINTEGER
dbd_db_commit(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    SQLINTEGER ret;
    SQLCHAR  *msg;

    ret = SQLTransact(henv,imp_dbh->hdbc,SQL_COMMIT);
    msg = (ERRTYPE(ret)  ? "Commit failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
    EOI(ret);
    return 1;
}

SQLINTEGER
dbd_db_rollback(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    SQLINTEGER ret;
    SQLCHAR  *msg;

    ret = SQLTransact(henv,imp_dbh->hdbc,SQL_ROLLBACK);
    msg = (ERRTYPE(ret)  ? "Rollback failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
    EOI(ret);

    return 1;
}


SQLINTEGER
dbd_db_disconnect(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
	SQLINTEGER ret;
	SQLCHAR  *msg;

    /* We assume that disconnect will always work	*/
    /* since most errors imply already disconnected.	*/
    DBIc_ACTIVE_off(imp_dbh);

    ret = SQLDisconnect(imp_dbh->hdbc);
    msg = (ERRTYPE(ret)  ? "Disconnect failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
    EOI(ret);

    ret = SQLFreeConnect(imp_dbh->hdbc);
    msg = (ERRTYPE(ret)  ? "Free connect failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
    EOI(ret);

    ret = SQLFreeEnv(henv);
    msg = (ERRTYPE(ret)  ? "Free henv failed" : "Invalid handle");
    ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
    EOI(ret);

    /* We don't free imp_dbh since a reference still exists	*/
    /* The DESTROY method is the only one to 'free' memory.	*/
    /* Note that statement objects may still exists for this dbh!	*/
    return 1;
}


void
dbd_db_destroy(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    if (DBIc_ACTIVE(imp_dbh))
		dbd_db_disconnect(dbh);
    /* Nothing in imp_dbh to be freed	*/
    DBIc_IMPSET_off(imp_dbh);
}


SQLINTEGER
dbd_db_STORE(dbh, keysv, valuesv)
    SV *dbh;
    SV *keysv;
    SV *valuesv;
{
    D_imp_dbh(dbh);
    STRLEN kl;
    SQLCHAR  *key = SvPV(keysv,kl);
    SV *cachesv = NULL;
    SQLINTEGER on = SvTRUE(valuesv);
	SQLINTEGER ret;
	SQLCHAR  *msg;
	
    if (kl==10 && strEQ(key, "AutoCommit")) {
		if ( on  ) {
			ret = SQLSetConnectOption(imp_dbh->hdbc,SQL_AUTOCOMMIT,
					(on ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF ));
			msg = (ERRTYPE(ret) ? "Change of AUTOCOMMIT failed" :
								  "Invaild Handle");
    		ret = check_error(dbh,ret,henv,imp_dbh->hdbc,NHSTMT,msg);
			EOI(ret);
		} else {
	    	cachesv = (on) ? &sv_yes : &sv_no;	/* cache new state */
		}
    } else {
		return FALSE;
    }
    if (cachesv) /* cache value for later DBI 'quick' fetch? */
		hv_store((HV*)SvRV(dbh), key, kl, cachesv, 0);
    return TRUE;
}


SV *
dbd_db_FETCH(dbh, keysv)
    SV *dbh;
    SV *keysv;
{
    /* D_imp_dbh(dbh); */
    STRLEN kl;
    SQLCHAR  *key = SvPV(keysv,kl);
    SV *retsv = NULL;
    /* Default to caching results for DBI dispatch quick_FETCH	*/
    SQLINTEGER cacheit = TRUE;

    if (1) {		/* no attribs defined yet	*/
	return Nullsv;
    }
    if (cacheit) {	/* cache for next time (via DBI quick_FETCH)	*/
	hv_store((HV*)SvRV(dbh), key, kl, retsv, 0);
	(void)SvREFCNT_inc(retsv);	/* so sv_2mortal won't free it	*/
    }
    return sv_2mortal(retsv);
}



/* ================================================================== */


SQLINTEGER
dbd_st_prepare(sth, statement, attribs)
    SV *sth;
    SQLCHAR  *statement;
	SV *attribs;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER ret =0;
	short params =0;
    SQLCHAR  *msg;

    imp_sth->done_desc = 0;

    ret = SQLAllocStmt(imp_dbh->hdbc,&imp_sth->phstmt);
    msg = "Statement allocation error";
    ret = check_error(sth,ret,henv,imp_dbh->hdbc,NHSTMT,msg);

    EOI(ret);

    ret = SQLPrepare(imp_sth->phstmt,statement,SQL_NTS);
    msg = "Statement preparation error";
    ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);

    EOI(ret);

	ret = SQLNumParams(imp_sth->phstmt,&params);
	msg = "Unable to determine number of parameters";
	ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
	EOI(ret);
	
	DBIc_NUM_PARAMS(imp_sth) = params;

	if (params > 0 ){
    	/* scan statement for '?', ':1' and/or ':foo' style placeholders*/	
    	dbd_preparse(imp_sth, statement); 
	} else {	/* assumming a parameterless select */
		dbd_describe(sth,imp_sth );
	}

    DBIc_IMPSET_on(imp_sth);
    return 1;
}


void
dbd_preparse(imp_sth, statement)
    imp_sth_t *imp_sth;
    SQLCHAR  *statement;
{
    bool in_literal = FALSE;
    SQLCHAR  *src, *start, *dest;
    phs_t phs_tpl;
    SV *phs_sv;
    SQLINTEGER idx=0, style=0, laststyle=0;

    /* allocate room for copy of statement with spare capacity	*/
    /* for editing ':1' SQLINTEGERo ':p1' so we can use obndrv.	*/
    imp_sth->statement = (SQLCHAR *)safemalloc(strlen(statement) + 
							(DBIc_NUM_PARAMS(imp_sth)*4));

    /* initialise phs ready to be cloned per placeholder	*/
    memset(&phs_tpl, '\0',sizeof(phs_tpl));
    phs_tpl.ftype = 1;	/* VARCHAR2 */

    src  = statement;
    dest = imp_sth->statement;
    while(*src) {
	if (*src == '\'')
	    in_literal = ~in_literal;
	if ((*src != ':' && *src != '?') || in_literal) {
	    *dest++ = *src++;
	    continue;
	}
	start = dest;			/* save name inc colon	*/ 
	*dest++ = *src++;
	if (*start == '?') {		/* X/Open standard	*/
	    sprintf(start,":p%d", ++idx); /* '?' -> ':1' (etc)	*/
	    dest = start+strlen(start);
	    style = 3;

	} else if (isDIGIT(*src)) {	/* ':1'		*/
	    idx = atoi(src);
	    *dest++ = 'p';		/* ':1'->':p1'	*/
	    if (idx > MAX_BIND_VARS || idx <= 0)
		croak("Placeholder :%d index out of range", idx);
	    while(isDIGIT(*src))
		*dest++ = *src++;
	    style = 1;

	} else if (isALNUM(*src)) {	/* ':foo'	*/
	    while(isALNUM(*src))	/* includes '_'	*/
		*dest++ = *src++;
	    style = 2;
	} else {			/* perhaps ':=' PL/SQL construct */
	    continue;
	}
	*dest = '\0';			/* handy for debugging	*/
	if (laststyle && style != laststyle)
	    croak("Can't mix placeholder styles (%d/%d)",style,laststyle);
	laststyle = style;
	if (imp_sth->bind_names == NULL)
	    imp_sth->bind_names = newHV();
	phs_tpl.sv = &sv_undef;
	phs_sv = newSVpv((SQLCHAR *)&phs_tpl, sizeof(phs_tpl));
	hv_store(imp_sth->bind_names, start, (STRLEN)(dest-start), phs_sv, 0);
	/* warn("bind_names: '%s'\n", start);	*/
    }
    *dest = '\0';
    if (imp_sth->bind_names) {
	DBIc_NUM_PARAMS(imp_sth) = (SQLINTEGER)HvKEYS(imp_sth->bind_names);
	if (dbis->debug >= 2)
	    fprintf(DBILOGFP, "scanned %d distinct placeholders\n",
		(SQLINTEGER)DBIc_NUM_PARAMS(imp_sth));
    }
}

SQLINTEGER
dbd_bind_ph(sth, ph_namesv, newvalue, attribs)
    SV *sth;
    SV *ph_namesv;
    SV *newvalue;
    SV *attribs;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth;
    SV **svp;
    STRLEN name_len;
    SQLCHAR  *name;
    phs_t *phs;

    STRLEN value_len;
    void  *value_ptr;
	SQLINTEGER ret;
	SQLCHAR  *msg;
	short param = SQL_PARAM_INPUT,
          ctype = SQL_C_DEFAULT, stype = SQL_CHAR, scale = 0;
	unsigned prec=0;
	SQLINTEGER nullok = SQL_NTS;

    if (SvNIOK(ph_namesv) ) {	/* passed as a number	*/
		SQLCHAR  buf[90];
		name = buf;
		sprintf(name, ":p%d", (int)SvIV(ph_namesv));
		name_len = strlen(name);
    } else {
		name = SvPV(ph_namesv, name_len);
    }

    if (dbis->debug >= 2)
		fprintf(DBILOGFP, "bind %s <== '%s' (attribs: %s)\n",
			name, SvPV(newvalue,na), SvPV(attribs,na) );

    svp = hv_fetch(imp_sth->bind_names, name, name_len, 0);
    if (svp == NULL)
		croak("dbd_bind_ph placeholder '%s' unknown", name);
    phs = (phs_t*)((void*)SvPVX(*svp));		/* placeholder struct	*/

    if (phs->sv == &sv_undef) {	 /* first bind for this placeholder	*/
	phs->sv = newSV(0);
	phs->ftype = 1;
    }

    if (attribs) {
		/* Setup / Clear attributes as defined by attribs.		*/
		/* If attribs is EMPTY then reset attribs to default.		*/
		;	/* XXX */
		if ( (svp =hv_fetch((HV*)SvRV(attribs), "Stype",5, 0)) != NULL) 
	    	stype = phs->ftype = SvIV(*svp);
        if ( (svp=hv_fetch((HV*)SvRV(attribs), "Ctype",5, 0)) != NULL) 
			ctype = SvIV(*svp);
        if ( (svp=hv_fetch((HV*)SvRV(attribs), "Prec",4, 0)) != NULL) 
			prec = SvIV(*svp);
        if ( (svp=hv_fetch((HV*)SvRV(attribs), "Scale",5, 0)) != NULL) 
			scale = SvIV(*svp);
        if ( (svp=hv_fetch((HV*)SvRV(attribs), "Nullok",6, 0)) != NULL) 
			nullok = SvIV(*svp);


    }	/* else if NULL / UNDEF then don't alter attributes.	*/
	/* This approach allows maximum performance when	*/
	/* rebinding parameters often (for multiple executes).	*/

    /* At the moment we always do sv_setsv() and rebind.	*/
    /* Later we may optimise this so that more often we can	*/
    /* just copy the value & length over and not rebind.	*/

    if (SvOK(newvalue)) {
		/* XXX need to consider oraperl null vs space issues?	*/
		/* XXX need to consider taking reference to source var	*/
		sv_setsv(phs->sv, newvalue);
		value_ptr = SvPV(phs->sv, value_len);
        phs->indp = 0;
    } else {
        sv_setsv(phs->sv,0); 
		value_ptr = "";
		value_len = 0;
        phs->indp = SQL_NULL_DATA;
    }

    if (!nullok && !SvOK(phs->sv)) {
		fprintf(stderr,"phs->sv is not OK\n");
	}	
	ret = SQLBindParameter(imp_sth->phstmt,(SQLINTEGER)SvIV(ph_namesv),
            param,ctype,stype,prec,scale,SvPVX(phs->sv),0,(phs->indp != 0 && nullok)?&phs->indp:NULL);
	msg = ( ERRTYPE(ret) ? "Bind failed" : "Invalid Handle");
	ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
	EOI(ret);

    return 1;
}

SQLINTEGER
dbd_describe(h, imp_sth)
    SV *h;
    imp_sth_t *imp_sth;
{
	D_imp_dbh_from_sth;
    SQLCHAR  *cbuf_ptr;
    SQLINTEGER t_cbufl=0;
    short f_cbufl[MAX_COLS];
    short num_fields;
    SQLINTEGER i, ret;
	SQLCHAR  *msg;

    if (imp_sth->done_desc)
		return 1;	/* success, already done it */
    imp_sth->done_desc = 1;

	ret = SQLNumResultCols(imp_sth->phstmt,&num_fields);
    msg = ( ERRTYPE(ret) ? "SQLNumResultCols failed" : "Invalid Handle");
    ret = check_error(h,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
    EOI(ret);
    DBIc_NUM_FIELDS(imp_sth) = num_fields;

    /* allocate field buffers				*/
    Newz(42, imp_sth->fbh,      num_fields, imp_fbh_t);
    /* allocate a buffer to hold all the column names	*/
    Newz(42, imp_sth->fbh_cbuf,(num_fields * (MAX_COL_NAME_LEN+1)), SQLCHAR );
    cbuf_ptr = (SQLCHAR  *)imp_sth->fbh_cbuf;

    /* Get number of fields and space needed for field names	*/
    for(i=0; i < num_fields; ++i ) {
		SQLCHAR  cbuf[MAX_COL_NAME_LEN];
		SQLCHAR  dbtype;
		imp_fbh_t *fbh = &imp_sth->fbh[i];
		f_cbufl[i] = sizeof(cbuf);

		ret = SQLDescribeCol(imp_sth->phstmt,i+1,cbuf_ptr,MAX_COL_NAME_LEN,
				&f_cbufl[i],&fbh->dbtype,&fbh->prec,&fbh->scale,&fbh->nullok);
		msg	= (ERRTYPE(ret) ? "DescribeCol failed" : "Invalid Handle");
		ret = check_error(imp_sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
		EOI(ret);
		ret = SQLColAttributes(imp_sth->phstmt,i+1,SQL_COLUMN_LENGTH,
				NULL, 0, NULL ,&fbh->dbsize);
		msg	= (ERRTYPE(ret) ? "ColAttributes failed" : "Invalid Handle");
		ret = check_error(imp_sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
		EOI(ret);
		ret = SQLColAttributes(imp_sth->phstmt,i+1,SQL_COLUMN_DISPLAY_SIZE,
				NULL, 0, NULL ,&fbh->dsize);
		msg	= (ERRTYPE(ret) ? "ColAttributes failed" : "Invalid Handle");
		ret = check_error(imp_sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
		EOI(ret);

		fbh->imp_sth = imp_sth;
		fbh->cbuf    = cbuf_ptr;
		fbh->cbufl   = f_cbufl[i];
		fbh->cbuf[fbh->cbufl] = '\0';	 /* ensure null terminated	*/
		cbuf_ptr += fbh->cbufl + 1;	 /* increment name poSQLINTEGERer	*/

		/* Now define the storage for this field data.			*/
	
		fbh->ftype = SQL_C_CHAR ;
		fbh->rlen = fbh->bufl  = fbh->dsize+1;/* +1: STRING null terminator	*/

		/* currently we use an sv, later we'll use an array	*/
		fbh->sv = newSV((STRLEN)fbh->bufl);
		(void)SvUPGRADE(fbh->sv, SVt_PV);
		SvREADONLY_on(fbh->sv);
		(void)SvPOK_only(fbh->sv);
		fbh->buf = (SQLCHAR  *)SvPVX(fbh->sv);
	
		/* BIND */
		ret = SQLBindCol(imp_sth->phstmt,i+1,SQL_C_CHAR,fbh->buf,
					fbh->bufl,&fbh->rlen);
		if (ret == SQL_SUCCESS_WITH_INFO ) {
			warn("BindCol error on %s: %d", fbh->cbuf);
		} else {
			msg = (ERRTYPE(ret) ? "BindCol failed" : "Invalid Handle");
			ret = check_error(imp_sth,ret,henv,imp_dbh->hdbc,
								imp_sth->phstmt,msg);
    		EOI(ret);
		}

		if (dbis->debug >= 2)
		    fbh_dump(fbh, i);
    }
    return 1;
}

SQLINTEGER
dbd_st_opts(sth, opt, value)
	SV *sth;
	IV	opt;
	IV	value;
{
	D_imp_sth(sth);
	D_imp_dbh_from_sth;
    SQLCHAR  *msg;
    SQLINTEGER ret ;

	ret = SQLSetStmtOption(imp_sth->phstmt,opt, value);
	msg = "SQLSetStmtOption failed";
	ret = check_error(sth,ret,henv, imp_dbh->hdbc, imp_sth->phstmt,msg);
	EOI(ret);

	return 1;
}

SQLINTEGER
dbd_st_execute(sth)
    SV *sth;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth;
	SQLCHAR  *msg;
	SQLINTEGER ret ;
 
    /* describe and allocate storage for results		*/
    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
        /* dbd_describe has already called check_error()		*/
		return 0;
    }
	ret = SQLExecute(imp_sth->phstmt);
	msg = "SQLExecute failed";
    ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
	EOI(ret);

    DBIc_ACTIVE_on(imp_sth);
    return 1;
}



AV *
dbd_st_fetch(sth)
    SV *	sth;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth;
    SQLINTEGER debug = dbis->debug;
    SQLINTEGER num_fields;
    SQLINTEGER i,ret;
    AV *av;
	SQLCHAR  *msg;

    /* Check that execute() was executed sucessfuly. This also implies	*/
    /* that dbd_describe() executed sucessfuly so the memory buffers	*/
    /* are allocated and bound.						*/
    if ( !DBIc_ACTIVE(imp_sth) ) {
		check_error(sth, -3 , NHENV,NHDBC,NHSTMT, "no statement executing");
		return Nullav;
    }
    
    if ((ret = SQLFetch(imp_sth->phstmt)) != SQL_SUCCESS ) {
		if (ret != SQL_NO_DATA_FOUND) {	/* was not just end-of-fetch	*/
			msg = (ERRTYPE(ret) ? "Fetch failed" : "Invalid Handle");
        	check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
			EOI(ret);
		}
		if (debug >= 3)
	    	fprintf(DBILOGFP, "    dbd_st_fetch failed, rc=%d",ret);
		return Nullav;
    }

    av = DBIS->get_fbav(imp_sth);
    num_fields = AvFILL(av)+1;

    if (debug >= 3)
	fprintf(DBILOGFP, "    dbd_st_fetch %d fields\n", num_fields);

    for(i=0; i < num_fields; ++i) {
		imp_fbh_t *fbh = &imp_sth->fbh[i];
		SV *sv = AvARRAY(av)[i]; /* Note: we reuse the supplied SV	*/

		if (fbh->rlen > -1) {	/* normal case - column is not null */
			SvCUR(fbh->sv) = fbh->rlen;
			sv_setsv(sv,fbh->sv);
		} else {				/*  column contains a null value */
       		fbh->indp = fbh->rlen;
			fbh->rlen = 0; 
			(void)SvOK_off(sv);
		} 
		if (debug >= 2)
	    	fprintf(DBILOGFP, "\t%d: rc=%d '%s'\n", i, ret, SvPV(sv,na));
    }
    return av;
}

SQLINTEGER
dbd_st_readblob(sth, field, offset, len, destrv, destoffset)
    SV *sth;
    SQLINTEGER field;
    long offset;
    long len;
    SV *destrv;
    long destoffset;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth;
    SQLINTEGER retl=0;
    SV *bufsv;
	SQLINTEGER rtval=0;
	SQLCHAR  *msg;
	
    bufsv = SvRV(destrv);
    sv_setpvn(bufsv,"",0);	/* ensure it's writable string	*/
    SvGROW(bufsv, len+destoffset+1);	/* SvGROW doesn't do +1	*/

	rtval = SQLGetData(imp_sth->phstmt,field,SQL_C_BINARY,
				 SvPVX(bufsv),len+destoffset,&retl);
    msg = (ERRTYPE(rtval) ? "GetData failed to read blob":"Invalid Handle");
	rtval = check_error(sth,rtval,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
	EOI(rtval);
	
    SvCUR_set(bufsv, len );
    *SvEND(bufsv) = '\0'; /* consistent with perl sv_setpvn etc	*/

    return 1;
}


SQLINTEGER
dbd_st_rows(sth)
    SV *sth;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth;
	SQLINTEGER rows, ret;
	SQLCHAR  *msg;

	ret = SQLRowCount(imp_sth->phstmt,&rows);
	msg = (ERRTYPE(ret) ? "SQLRowCount failed" : "Invaild Handle");
    ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
	EOI(ret);
    return(rows);
}


SQLINTEGER
dbd_st_finish(sth)
    SV *sth;
{
    D_imp_sth(sth);
	D_imp_dbh_from_sth; 
	SQLINTEGER ret;
	SQLCHAR  *msg;

	if (DBIc_ACTIVE(imp_sth) ) {
		ret = SQLCancel(imp_sth->phstmt);
		msg = (ERRTYPE(ret) ? "SQLCancel failed" : "Invaild Handle");
    	ret = check_error(sth,ret,henv,imp_dbh->hdbc,imp_sth->phstmt,msg);
		EOI(ret);
	}
    DBIc_ACTIVE_off(imp_sth);
    return 1;
}


void
dbd_st_destroy(sth)
    SV *sth;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER i;
    /* Check if an explicit disconnect() or global destruction has	*/
    /* disconnected us from the database before attempting to close.	*/

    /* Free off contents of imp_sth	*/

    for(i=0; i < DBIc_NUM_FIELDS(imp_sth); ++i) {
	imp_fbh_t *fbh = &imp_sth->fbh[i];
	sv_free(fbh->sv);
    }
    Safefree(imp_sth->fbh);
    Safefree(imp_sth->fbh_cbuf);
    Safefree(imp_sth->statement);

    if (imp_sth->bind_names) {
	HV *hv = imp_sth->bind_names;
	SV *sv;
	SQLCHAR  *key;
	I32 retlen;
	hv_iterinit(hv);
	while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
	    phs_t *phs_tpl;
	    if (sv != &sv_undef) {
		phs_tpl = (phs_t*)SvPVX(sv);
		sv_free(phs_tpl->sv);
	    }
	}
	sv_free((SV*)imp_sth->bind_names);
    }
/*
 * Chet Murthy's patch for DB2 heap overflow problems
 */
    i = SQLFreeStmt (imp_sth->phstmt, SQL_DROP);
    i = check_error(NULL,i,henv,imp_dbh->hdbc,NHSTMT,
				   				"Statement destruction error");
/* End Chet */
    DBIc_IMPSET_off(imp_sth);		/* let DBI know we've done it	*/
}


SQLINTEGER
dbd_st_STORE(sth, keysv, valuesv)
    SV *sth;
    SV *keysv;
    SV *valuesv;
{
    D_imp_sth(sth);
    STRLEN kl;
    SQLCHAR  *key = SvPV(keysv,kl);
    SV *cachesv = NULL;
    SQLINTEGER on = SvTRUE(valuesv);

    if (kl==4 && strEQ(key, "long")) {
		imp_sth->long_buflen = SvIV(valuesv);

    } else if (kl==5 && strEQ(key, "trunc")) {
		imp_sth->long_trunc_ok = on;

    } else {
		return FALSE;
    }
    if (cachesv) /* cache value for later DBI 'quick' fetch? */
		hv_store((HV*)SvRV(sth), key, kl, cachesv, 0);
    return TRUE;
}


SV *
dbd_st_FETCH(sth, keysv)
    SV *sth;
    SV *keysv;
{
    D_imp_sth(sth);
    STRLEN kl;
    SQLCHAR  *key = SvPV(keysv,kl);
    SQLINTEGER i;
    SV *retsv = NULL;
    /* Default to caching results for DBI dispatch quick_FETCH	*/
    SQLINTEGER cacheit = TRUE;

    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
	/* dbd_describe has already called ora_error()		*/
        return Nullsv;	/* XXX not quite the right thing to do?	*/
    }

    i = DBIc_NUM_FIELDS(imp_sth);

	if (kl==7 && strEQ(key, "lengths")) {
		AV *av = newAV();
		retsv = newRV((SV*)av);
		while(--i >= 0)
	    	av_store(av, i, newSViv((IV)imp_sth->fbh[i].dsize));
    } else  {
		 if (kl==5 && strEQ(key, "types")) {
			AV *av = newAV();
			retsv = newRV((SV*)av);
			while(--i >= 0)
	    		av_store(av, i, newSViv(imp_sth->fbh[i].dbtype));

    	} else  {
			if (kl==13 && strEQ(key, "Num_OF_Params")) {
				HV *bn = imp_sth->bind_names;
				retsv = newSViv( (bn) ? HvKEYS(bn) : 0 );

    		} else if (kl==4 && strEQ(key, "NAME")) {
				AV *av = newAV();
				retsv = newRV((SV*)av);
				while(--i >= 0)
		    		av_store(av, i, newSVpv((SQLCHAR *)imp_sth->fbh[i].cbuf,0));

    		} else {
				return Nullsv;
    		}
		}
	}
    if (cacheit) { /* cache for next time (via DBI quick_FETCH)	*/
	 	SV **svp = hv_fetch((HV*)SvRV(sth), key, kl, 1);
 		sv_free(*svp);
 		*svp = retsv;
		(void)SvREFCNT_inc(retsv);	/* so sv_2mortal won't free it	*/
    }
    return sv_2mortal(retsv);
}

