/*
   dbdimp.c, engn_perldb2, db2_v71, 1.4 00/04/14 16:07:13

   Copyright (c) 1995,1996,1997,1998,1999,2000 International Business Machines Corp.
*/

#include <stdio.h>
#include "DB2.h"

#define EOI(x)  if (x < 0 || x == 100) return (0)
#define NHENV   SQL_NULL_HENV
#define NHDBC   SQL_NULL_HDBC
#define NHSTMT  SQL_NULL_HDBC
#define ERRTYPE(x)    ((x == 1) && (x == SQL_ERROR))

DBISTATE_DECLARE;

void
dbd_init(dbistate)
    dbistate_t *dbistate;
{
    DBIS = dbistate;
}

void
do_error(SV *h, SQLINTEGER rc, SQLHENV h_env, SQLHDBC h_conn, SQLHSTMT h_stmt,
     char *what)
{
    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);
    short length;
    SV *state = DBIc_STATE(imp_xxh);
    SQLINTEGER  sqlcode;
    SQLCHAR  sqlstate[6];
    char msg[SQL_MAX_MESSAGE_LENGTH+1];
    SQLINTEGER msgsize = SQL_MAX_MESSAGE_LENGTH+1;

    msg[0]='\0';
    if (h_env != NHENV) {
        SQLError(h_env,h_conn,h_stmt, sqlstate, &sqlcode, (SQLCHAR *)msg,
                 msgsize,&length);
    } else {
        strcpy((char *)msg, (char *)what);
    }

    sv_setiv(DBIc_ERR(imp_xxh), (IV)sqlcode);
    sv_setpv(errstr, (char *)msg);
    sv_setpv(state,(char  *)sqlstate);
    if (what && (h_env == NHENV)) {
        sv_catpv(errstr, " (DBD: ");
        sv_catpv(errstr, (char  *)what);
        sv_catpv(errstr, ")");
    }
    DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);
    if (DBIS->debug >= 2)
        fprintf(DBILOGFP, "%s error %d recorded: %s\n",
                what, rc, SvPV(errstr,na));
}

int
check_error(h, rc, what)
SV *h;
IV rc;
char *what;
{
    D_imp_xxh(h);
    struct imp_dbh_st *imp_dbh = NULL;
    struct imp_sth_st *imp_sth = NULL;
    SQLHENV h_env = SQL_NULL_HENV;
    SQLHDBC h_conn = SQL_NULL_HDBC;
    SQLHSTMT h_stmt = SQL_NULL_HSTMT;

    if (rc == SQL_SUCCESS || rc == SQL_NO_DATA) {
        return(rc);
    }
    switch(DBIc_TYPE(imp_xxh)) {
        case DBIt_ST:
            imp_sth = (struct imp_sth_st *)(imp_xxh);
            imp_dbh = (struct imp_dbh_st *)(DBIc_PARENT_COM(imp_sth));
            h_stmt = imp_sth->phstmt;
            break;
        case DBIt_DB:
            imp_dbh = (struct imp_dbh_st *)(imp_xxh);
            break;
        default:
            croak("panic dbd_error on bad handle type");
    }
    h_conn = imp_dbh->hdbc;
    h_env = imp_dbh->henv;

    if (h_env == NHENV) {
        do_error(h,rc,NHENV,NHDBC,NHSTMT,what);
    } else if (rc == SQL_SUCCESS_WITH_INFO) {
        if (DBIS->debug > 2) {
               do_error(h,rc,h_env,h_conn,h_stmt,what);
        }
    } else {
        do_error(h,rc,h_env,h_conn,h_stmt,what);
    }
    return((rc == SQL_SUCCESS_WITH_INFO ? SQL_SUCCESS : rc));
}

void
fbh_dump(fbh, i)
    imp_fbh_t *fbh;
    int i;
{
    FILE *fp = DBILOGFP;
    fprintf(fp, "fbh %d: '%s' %s, ",
        i, fbh->cbuf, (fbh->nullok) ? "NULLable" : "" );
    fprintf(fp, "type %d,  %ld, dsize %ld, p%d s%d\n",
        fbh->dbtype, (long)fbh->dsize, fbh->prec, fbh->scale );
    fprintf(fp, "   out: ftype %d, indp %d, bufl %d, rlen %d\n",
        fbh->ftype, fbh->indp, fbh->bufferSize, fbh->rlen );
}

static int SQLTypeIsLong( SQLSMALLINT SQLType )
{
  if( SQL_LONGVARBINARY == SQLType ||
      SQL_LONGVARCHAR == SQLType ||
      SQL_LONGVARGRAPHIC == SQLType ||
      SQL_BLOB == SQLType ||
      SQL_CLOB == SQLType ||
      SQL_DBCLOB == SQLType )
    return 1;

  return 0;
}

static int SQLTypeIsBinary( SQLSMALLINT SQLType )
{
  if( SQL_BINARY == SQLType ||
      SQL_VARBINARY == SQLType ||
      SQL_LONGVARBINARY == SQLType ||
      SQL_BLOB == SQLType )
    return 1;

  return 0;
}

static int SQLTypeIsGraphic( SQLSMALLINT SQLType )
{
  if( SQL_GRAPHIC == SQLType ||
      SQL_VARGRAPHIC == SQLType ||
      SQL_LONGVARGRAPHIC == SQLType ||
      SQL_DBCLOB == SQLType )
    return 1;

  return 0;
}

static int GetTrimmedSpaceLen( SQLCHAR *string, int len )
{
   int i = 0;
   int trimmedLen = 0;
   int charLen;

   if( !string || len <= 0 )
      return 0;

   do
   {
     charLen = mblen( (char*)string + i, MB_CUR_MAX );

     if( charLen <= 0 ) /* invalid multi-byte character (<0) or embedded    */
        charLen = 1;    /* null (=0), just skip this byte                   */

     if( charLen > 1 || string[i] != ' ' )
       /* record length of string up to end of current character */
       trimmedLen = i + charLen;

     i += charLen;      /* advance to next character */
   } while( charLen == 0 || i >= len );

   return trimmedLen;
}

/* ================================================================== */


/* ================================================================== */

int
dbd_db_connect(dbh,imp_dbh,dbname,uid,pwd)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    SQLCHAR  *dbname,*uid,*pwd;
{
    D_imp_drh_from_dbh;
    char *msg;
    SQLINTEGER ret;

    ret = SQLAllocHandle(SQL_HANDLE_DBC, imp_drh->henv,
                         &imp_dbh->hdbc);
    msg = (ERRTYPE(ret) ? "Connect allocation failed" :
                                "Invalid Handle");
    ret = check_error(dbh,ret,msg);
    if (ret != SQL_SUCCESS) {
        if (imp_drh->connects == 0) {
            SQLFreeEnv(imp_drh->henv);
            imp_drh->henv = NHENV;
        }
        return(ret);        /* Must return SQL codes not perl/DBD/DBI */
    }                        /* otherwise failure is not caught  */

    if (DBIS->debug >= 2)
        fprintf(DBILOGFP, "connect '%s', '%s', '%s'", dbname, uid, pwd);

    ret = SQLConnect(imp_dbh->hdbc,dbname,SQL_NTS,uid,SQL_NTS,pwd,SQL_NTS);
    msg = ( ERRTYPE(ret) ? "Connect failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    if (ret != SQL_SUCCESS) {
        SQLFreeConnect(imp_dbh->hdbc);
        if (imp_drh->connects == 0) {
            SQLFreeEnv(imp_drh->henv);
            imp_drh->henv = SQL_NULL_HENV;
        }
        return(ret);            /* Must return SQL codes not perl/DBD/DBI */
    }                             /* otherwise failure is not caught  */
    /* DBI spec requires AutoCommit on */
    ret = SQLSetConnectAttr(imp_dbh->hdbc, SQL_ATTR_AUTOCOMMIT,
                            (void *)SQL_AUTOCOMMIT_ON, 0);
    msg = (ERRTYPE(ret) ? "SetConnectAttr Failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    if (ret != SQL_SUCCESS) {
        SQLFreeConnect(imp_dbh->hdbc);
        if (imp_drh->connects == 0) {
            SQLFreeEnv(imp_drh->henv);
            imp_drh->henv = NHENV;
        }
        return(ret);        /* Must return SQL codes not perl/DBD/DBI */
    }                       /* otherwise failure is not caught  */

    DBIc_set( imp_dbh, DBIcf_AutoCommit, 1);

    return 1;
}

int
dbd_db_login(dbh, imp_dbh, dbname, uid, pwd)
    SV *dbh;
    imp_dbh_t *imp_dbh;
    char  *dbname;
    char  *uid;
    char  *pwd;
{
    D_imp_drh_from_dbh;
    SQLINTEGER ret;
    char *msg;

    if (! imp_drh->connects) {
        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
                             &imp_drh->henv);
        msg = (imp_drh->henv == NHENV ?
                "Total Environment allocation failure!" :
                "Environment allocation failed");
        ret = check_error(dbh,ret,msg);
        EOI(ret);

        /* If an application is run as an ODBC application, the         */
        /* SQL_ATTR_ODBC_VERSION environment attribute must be set;     */
        /* otherwise, an error will be returned when an attempt is      */
        /* made to allocate a connection handle.                        */
        ret = SQLSetEnvAttr( imp_drh->henv, SQL_ATTR_ODBC_VERSION,
                             (SQLPOINTER) SQL_OV_ODBC3, 0 );
        msg = "SQLSetEnvAttr failed";
        ret = check_error(dbh, ret, msg);
        EOI(ret);
    }
    imp_dbh->henv = imp_drh->henv;
    ret = dbd_db_connect(dbh,imp_dbh,dbname, uid, pwd);
    EOI(ret);
    imp_drh->connects++;

    DBIc_IMPSET_on(imp_dbh);    /* imp_dbh set up now            */
    DBIc_ACTIVE_on(imp_dbh);    /* call disconnect before freeing    */
    return 1;
}


int
dbd_db_do(dbh, statement) /* error : <=(-2), ok row count : >=0, unknown count : (-1)   */
    SV *dbh;
    char *statement;
{
    D_imp_dbh(dbh);
    SQLINTEGER ret, rows;
    char *msg;
    SQLHSTMT stmt;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, imp_dbh->hdbc,
                         &stmt);
    msg = "Statement allocation error";
    ret = check_error(dbh, ret, msg);
    if (ret < 0)
        return(-2);

    ret = SQLExecDirect(stmt, (SQLCHAR *)statement, SQL_NTS);
    msg = "Execute immediate failed";
    ret = check_error(dbh, ret, msg);
    if (ret < 0)
        rows = -2;
    else {
        ret = SQLRowCount(stmt, &rows);
        msg = "SQLRowCount failed";
        ret = check_error(dbh, ret, msg);
        if (ret < 0)
            rows = -1;
    }

    ret = SQLFreeStmt(stmt, SQL_DROP);
    msg = "Statement destruction error";
    (void) check_error(dbh, ret, msg);

    return rows;
}


int
dbd_db_commit(dbh,imp_dbh)
    SV *dbh;
    imp_dbh_t    *imp_dbh;
{
    SQLINTEGER ret;
    char *msg;

    ret = SQLTransact(imp_dbh->henv,imp_dbh->hdbc,SQL_COMMIT);
    msg = (ERRTYPE(ret)  ? "Commit failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    EOI(ret);
    return 1;
}

int
dbd_db_rollback(dbh,imp_dbh)
    SV *dbh;
    imp_dbh_t    *imp_dbh;
{
    SQLINTEGER ret;
    char *msg;

    ret = SQLTransact(imp_dbh->henv,imp_dbh->hdbc,SQL_ROLLBACK);
    msg = (ERRTYPE(ret)  ? "Rollback failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    EOI(ret);

    return 1;
}


int
dbd_db_disconnect(dbh,imp_dbh)
    SV *dbh;
    imp_dbh_t *imp_dbh;
{
    D_imp_drh_from_dbh;
    SQLINTEGER ret;
    char *msg;

    ret = SQLDisconnect(imp_dbh->hdbc);
    msg = (ERRTYPE(ret)  ? "Disconnect failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    EOI(ret);

    /* Only turn off the ACTIVE attribute of the database handle        */
    /* if SQLDisconnect() was successful.  If it wasn't successful,     */
    /* we still have a connection!                                      */

    DBIc_ACTIVE_off(imp_dbh);

    ret = SQLFreeConnect(imp_dbh->hdbc);
    msg = (ERRTYPE(ret)  ? "Free connect failed" : "Invalid handle");
    ret = check_error(dbh,ret,msg);
    EOI(ret);

    imp_dbh->hdbc = SQL_NULL_HDBC;
    imp_drh->connects--;
    if (imp_drh->connects == 0) {
        ret = SQLFreeEnv(imp_drh->henv);
        msg = (ERRTYPE(ret)  ? "Free henv failed" : "Invalid handle");
        ret = check_error(dbh,ret,msg);
        EOI(ret);
    }

    /* We don't free imp_dbh since a reference still exists    */
    /* The DESTROY method is the only one to 'free' memory.    */
    /* Note that statement objects may still exists for this dbh!    */
    return 1;
}


void
dbd_db_destroy(dbh)
    SV *dbh;
{
    D_imp_dbh(dbh);
    if (DBIc_ACTIVE(imp_dbh))
        dbd_db_disconnect(dbh,imp_dbh);
    /* Nothing in imp_dbh to be freed    */
    DBIc_IMPSET_off(imp_dbh);
}


int
dbd_db_STORE(dbh, keysv, valuesv)
    SV *dbh;
    SV *keysv;
    SV *valuesv;
{
    D_imp_dbh(dbh);
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    SV *cachesv = NULL;
    int on = SvTRUE(valuesv);
    SQLINTEGER ret;
    char *msg;

    if (kl==10 && strEQ(key, "AutoCommit")) {
        ret = SQLSetConnectAttr(imp_dbh->hdbc,
                                SQL_ATTR_AUTOCOMMIT,
              (on ? (void *)SQL_AUTOCOMMIT_ON : (void *)SQL_AUTOCOMMIT_OFF),
                                0);
        if ( ret ) {
            msg = (ERRTYPE(ret) ? "Change of AUTOCOMMIT failed" :
                                  "Invalid Handle");
            ret = check_error(dbh,ret,msg);
            cachesv = &sv_undef;
        } else {
            cachesv = (on) ? &sv_yes : &sv_no;    /* cache new state */
            DBIc_set(imp_dbh, DBIcf_AutoCommit, on);
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
    char *key = SvPV(keysv,kl);
    SV *retsv = NULL;
    /* Default to caching results for DBI dispatch quick_FETCH    */
    int cacheit = TRUE;

    if (1) {        /* no attribs defined yet    */
      return Nullsv;
    }
    if (cacheit) {    /* cache for next time (via DBI quick_FETCH)    */
      hv_store((HV*)SvRV(dbh), key, kl, retsv, 0);
      (void)SvREFCNT_inc(retsv);    /* so sv_2mortal won't free it    */
    }
    return sv_2mortal(retsv);
}



/* ================================================================== */


int
dbd_st_prepare(sth, statement, attribs)
    SV *sth;
    char *statement;
    SV *attribs;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER ret =0;
    short params =0;
    char *msg;

    imp_sth->done_desc = 0;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, imp_dbh->hdbc,
                         &imp_sth->phstmt);
    msg = "Statement allocation error";
    ret = check_error(sth,ret,msg);

    EOI(ret);

    ret = SQLPrepare(imp_sth->phstmt,(SQLCHAR *)statement,SQL_NTS);
    msg = "Statement preparation error";
    ret = check_error(sth,ret,msg);

    EOI(ret);

    if (DBIS->debug >= 2)
        fprintf(DBILOGFP, "    dbd_st_prepare'd sql f%d\n\t%s\n",
                imp_sth->phstmt, statement);

    ret = SQLNumParams(imp_sth->phstmt,&params);
    msg = "Unable to determine number of parameters";
    ret = check_error(sth,ret,msg);
    EOI(ret);

    DBIc_NUM_PARAMS(imp_sth) = params;

    if (params > 0 ){
        /* scan statement for '?', ':1' and/or ':foo' style placeholders*/
        dbd_preparse(imp_sth, statement);
    } else {    /* assuming a parameterless select */
        dbd_describe(sth,imp_sth );
    }

    /* initialize sth pointers */
    imp_sth->RowCount = -1;
    imp_sth->bHasInput = 0;
    imp_sth->bHasOutput = 0;

    DBIc_IMPSET_on(imp_sth);
    return 1;
}


void
dbd_preparse(imp_sth, statement)
    imp_sth_t *imp_sth;
    char *statement;
{
    bool in_literal = FALSE;
    SQLCHAR  *src, *start, *dest;
    phs_t phs_tpl;
    SV *phs_sv;
    int idx=0, style=0, laststyle=0;

    /* allocate room for copy of statement with spare capacity    */
    /* for editing ':1' SQLINTEGERo ':p1' so we can use obndrv.    */
    imp_sth->statement = (SQLCHAR *)safemalloc(strlen(statement) +
                            (DBIc_NUM_PARAMS(imp_sth)*4));

    /* initialise phs ready to be cloned per placeholder    */
    memset(&phs_tpl, '\0',sizeof(phs_tpl));
    phs_tpl.sv = &sv_undef;

    src  = (SQLCHAR *)statement;
    dest = imp_sth->statement;
    while(*src) {
      if (*src == '\'')
          in_literal = ~in_literal;
      if ((*src != ':' && *src != '?') || in_literal) {
          *dest++ = *src++;
          continue;
      }
      start = dest;            /* save name inc colon    */
      *dest++ = *src++;
      if (*start == '?') {        /* X/Open standard    */
          sprintf((char *)start,":p%d", ++idx); /* '?' -> ':1' (etc)*/
          dest = start+strlen((char *)start);
          style = 3;

      } else if (isDIGIT(*src)) {    /* ':1'        */
          idx = atoi((char *)src);
          *dest++ = 'p';        /* ':1'->':p1'    */
          if (idx > MAX_BIND_VARS || idx <= 0)
          croak("Placeholder :%d index out of range", idx);
          while(isDIGIT(*src))
          *dest++ = *src++;
          style = 1;

      } else if (isALNUM(*src)) {    /* ':foo'    */
          while(isALNUM(*src))    /* includes '_'    */
          *dest++ = *src++;
          style = 2;
      } else {            /* perhaps ':=' PL/SQL construct */
          continue;
      }
      *dest = '\0';            /* handy for debugging    */
      if (laststyle && style != laststyle)
          croak("Can't mix placeholder styles (%d/%d)",style,laststyle);
      laststyle = style;
      if (imp_sth->bind_names == NULL)
          imp_sth->bind_names = newHV();
      phs_sv = newSVpv((char *)&phs_tpl, sizeof(phs_tpl));
      hv_store(imp_sth->bind_names, (char *)start,
                               (STRLEN)(dest-start), phs_sv, 0);
      /* warn("bind_names: '%s'\n", start);    */
    }
    *dest = '\0';
    if (imp_sth->bind_names) {
      DBIc_NUM_PARAMS(imp_sth) = (SQLINTEGER)HvKEYS(imp_sth->bind_names);
      if (DBIS->debug >= 2)
        fprintf(DBILOGFP, "scanned %d distinct placeholders\n",
                (SQLINTEGER)DBIc_NUM_PARAMS(imp_sth));
    }
}

int
dbd_bind_ph( SV *sth,
             imp_sth_t *imp_sth,
             SV *param,
             SV *value,
             IV sql_type,
             SV *attribs,
             int is_inout,
             IV maxlen )
{
    D_imp_dbh_from_sth;
    SV **svp;
    STRLEN name_len;
    SQLCHAR  *name;
    phs_t *phs;
    SQLUSMALLINT pNum;

    STRLEN value_len;
    SQLINTEGER ret;
    char *msg;
    short ctype = 0,
          scale = 0;
    unsigned prec = 0;
    short bFile = 0; /* Boolean indicating value is a file name for LOB input */
    SQLCHAR  buf[50];
    static SQLUINTEGER FileOptions = SQL_FILE_READ;
    static SQLINTEGER NullIndicator = SQL_NULL_DATA;

    if (SvNIOK(param) ) {    /* passed as a number    */
        name = buf;
        pNum = (int)SvIV(param);
        sprintf( (char *)name, ":p%d", pNum );
        name_len = strlen((char *)name);
    } else {
        name = (SQLCHAR *)SvPV(param, name_len);
        pNum = atoi( name + 2 );
    }

    if (DBIS->debug >= 2)
        fprintf(DBILOGFP, "bind %s <== '%s' (attribs: %s)\n",
            name, SvPV(value,na), attribs ? SvPV(attribs,na) : "<no attribs>" );

    svp = hv_fetch(imp_sth->bind_names, (char *)name, name_len, 0);
    if (svp == NULL)
        croak("Can't bind unknown parameter marker '%s'", name);
    phs = (phs_t*)((void*)SvPVX(*svp));        /* placeholder struct    */

    if( NULL != phs->sv )
    {
      /* We've used this placeholder before,
         decrement reference and set to undefined */
      SvREFCNT_dec( phs->sv );
      phs->sv = NULL;
    }

    /* Intialize parameter type to default value */
    if( is_inout )
      phs->paramType = SQL_PARAM_INPUT_OUTPUT;
    else
      phs->paramType = SQL_PARAM_INPUT;

    if (attribs)
    {
        /* Setup / Clear attributes as defined by attribs.        */
        /* If attribs is EMPTY then attribs are defaulted.        */
        if( is_inout &&
            (svp =hv_fetch((HV*)SvRV(attribs), "ParamT", 6, 0)) != NULL )
            phs->paramType = SvIV(*svp);
        if( (svp =hv_fetch((HV*)SvRV(attribs), "Stype", 5, 0)) != NULL )
            sql_type = SvIV(*svp);
        if( (svp=hv_fetch((HV*)SvRV(attribs), "Ctype", 5, 0)) != NULL )
            ctype = SvIV(*svp);
        if( (svp=hv_fetch((HV*)SvRV(attribs), "Prec", 4, 0)) != NULL )
            prec = SvIV(*svp);
        if( (svp=hv_fetch((HV*)SvRV(attribs), "Scale", 5, 0)) != NULL )
            scale = SvIV(*svp);
        if( (svp=hv_fetch((HV*)SvRV(attribs), "File", 4, 0)) != NULL )
            bFile = SvIV(*svp);
    } /* else if NULL / UNDEF then default to values assigned at top */
    /* This approach allows maximum performance when    */
    /* rebinding parameters often (for multiple executes).    */

    /* If the SQL type or scale haven't been specified, try to      */
    /* describe the parameter.  If this fails (it's an unregistered */
    /* stored proc for instance) then defaults will be used         */
    /* (SQL_VARCHAR and 0)                                          */
    if( 0 == sql_type || 0 == scale )
    {
      if( !phs->bDescribed )
      {
        SQLRETURN   rc;
        SQLSMALLINT nullable;

        phs->bDescribed = TRUE;
        rc = SQLDescribeParam( imp_sth->phstmt,
                               pNum,
                               &phs->descSQLType,
                               &phs->descColumnSize,
                               &phs->descDecimalDigits,
                               &nullable );
        phs->bDescribeOK = ( SQL_SUCCESS == rc ||
                             SQL_SUCCESS_WITH_INFO == rc );
      }

      if( phs->bDescribeOK )
      {
        if( 0 == sql_type )
          sql_type = phs->descSQLType;

        if( 0 == scale )
          scale = phs->descDecimalDigits;
      }
    }

    if( 0 == sql_type )
    {
      /* Still don't have an SQL type?  Set to default */
      sql_type = SQL_VARCHAR;
    }

    if( 0 == ctype )
    {
      /* Don't have a ctype yet?  Set to binary or char */
      if( SQLTypeIsBinary( sql_type ) )
        ctype = SQL_C_BINARY;
      else
        ctype = SQL_C_CHAR;
    }

    /* At the moment we always do sv_setsv() and rebind.    */
    /* Later we may optimise this so that more often we can */
    /* just copy the value & length over and not rebind.    */

    if( is_inout )
    {
      phs->sv = value;             /* Make a reference to the input variable */
      SvREFCNT_inc( value );       /* Increment reference to variable */
      if( SQL_PARAM_INPUT != phs->paramType )
        imp_sth->bHasOutput = 1;
      if( SQL_PARAM_OUTPUT != phs->paramType )
        imp_sth->bHasInput = 1;
      if( maxlen > 0 )
      {
        maxlen++;                  /* Add one for potential null terminator */
        /* Allocate new buffer only if current buffer isn't big enough */
        if( maxlen > phs->bufferSize )
        {
          if( 0 == phs->bufferSize ) /* new buffer */
            Newc( 1, phs->buffer, maxlen, SQLCHAR, void* );
          else
            Renewc( phs->buffer, maxlen, SQLCHAR, void* );
          phs->bufferSize = maxlen;
        }
      }
    }
    else if( SvOK( value ) )
    {
      SvPV( value, value_len );  /* Get value length */
      phs->indp = value_len;
      if( value_len > 0 )
      {
        if( value_len > phs->bufferSize )
        {
          if( 0 == phs->bufferSize ) /* new buffer */
            Newc( 1, phs->buffer, value_len+1, SQLCHAR, void* );
          else
            Renewc( phs->buffer, value_len+1, SQLCHAR, void* );
          phs->bufferSize = value_len+1;
        }

        memcpy( phs->buffer, SvPVX( value ), value_len );
        ((char*)phs->buffer)[value_len] = '\0'; /* null terminate */
      }
      maxlen = 0;
    }
    else
    {
      phs->indp = SQL_NULL_DATA;
      maxlen = 0;
    }

    if (DBIS->debug >= 2)
      fprintf( DBILOGFP,
               "  bind %s: "
               "ParmType=%d, "
               "Ctype=%d, "
               "SQLtype=%d, "
               "Prec=%d, "
               "Scale=%d, "
               "Maxlen=%d, "
               "%s\n",
               name,
               phs->paramType,
               ctype,
               sql_type,
               prec,
               scale,
               maxlen,
               !phs->bDescribed ? "Not described"
                                : ( phs->bDescribeOK ? "Described"
                                                     : "Desribe failed" ) );


    if( bFile &&
        SQL_PARAM_INPUT == phs->paramType &&
        ( SQL_BLOB == sql_type ||
          SQL_CLOB == sql_type ||
          SQL_DBCLOB == sql_type ) )
    {
      ret = SQLBindFileToParam( imp_sth->phstmt,
                                (SQLUSMALLINT)SvIV( param ),
                                sql_type,
                                ( phs->indp != SQL_NULL_DATA )
                                  ? phs->buffer
                                  : "",  /* Can't pass NULL */
                                NULL,
                                &FileOptions,
                                255,
                                ( phs->indp != SQL_NULL_DATA )
                                  ? NULL
                                  : &NullIndicator );
    }
    else
    {
      ret = SQLBindParameter( imp_sth->phstmt,
                              (SQLUSMALLINT)SvIV( param ),
                              phs->paramType,
                              ctype,
                              sql_type,
                              phs->bDescribeOK ? phs->descColumnSize : prec,
                              scale,
                              phs->buffer,
                              maxlen,
                              &phs->indp );
    }
    msg = ERRTYPE(ret) ? "Bind failed" : "Invalid Handle";
    ret = check_error( sth, ret, msg );
    EOI(ret);

    return 1;
}

int
dbd_describe(h, imp_sth)
    SV *h;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    SQLCHAR  *cbuf_ptr;
    SQLINTEGER t_cbufl=0;
    short num_fields;
    SQLINTEGER i, ret;
    char *msg;
    imp_fbh_t *fbh;

    if (imp_sth->done_desc)
        return 1;    /* success, already done it */
    imp_sth->done_desc = 1;

    ret = SQLNumResultCols(imp_sth->phstmt,&num_fields);
    msg = ( ERRTYPE(ret) ? "SQLNumResultCols failed" : "Invalid Handle");
    ret = check_error(h,ret,msg);
    EOI(ret);
    DBIc_NUM_FIELDS(imp_sth) = num_fields;

    /* allocate field buffers                */
    Newz(42, imp_sth->fbh, num_fields, imp_fbh_t);
    /* allocate a buffer to hold all the column names    */
    Newz(42, imp_sth->fbh_cbuf,
        (num_fields * (MAX_COL_NAME_LEN+1)), SQLCHAR );
    cbuf_ptr = imp_sth->fbh_cbuf;

    /* Get number of fields and space needed for field names    */
    for(i=0; i < num_fields; ++i )
    {
        fbh = &imp_sth->fbh[i];
        fbh->cbufl = MAX_COL_NAME_LEN+1;

        ret = SQLDescribeCol( imp_sth->phstmt,
                              i+1,
                              cbuf_ptr,
                              MAX_COL_NAME_LEN,
                              &fbh->cbufl,
                              &fbh->dbtype,
                              &fbh->prec,
                              &fbh->scale,
                              &fbh->nullok );
        msg    = (ERRTYPE(ret) ? "DescribeCol failed" : "Invalid Handle");
        ret = check_error(h,ret,msg);
        EOI(ret);
        fbh->imp_sth = imp_sth;
        fbh->cbuf    = cbuf_ptr;
        fbh->cbuf[fbh->cbufl] = '\0';     /* ensure null terminated    */
        cbuf_ptr += fbh->cbufl + 1;       /* increment name pointer    */

        /* Now define the storage for this field data.            */

        if( SQL_BINARY == fbh->dbtype ||
            SQL_VARBINARY == fbh->dbtype ||
            SQL_LONGVARBINARY == fbh->dbtype ||
            SQL_BLOB == fbh->dbtype )
        {
            fbh->ftype = SQL_C_BINARY;
            fbh->rlen = fbh->bufferSize = fbh->dsize = fbh->prec;
        }
        else
        {
            fbh->ftype = SQL_C_CHAR;
            ret = SQLColAttribute( imp_sth->phstmt,
                                   i+1,
                                   SQL_DESC_DISPLAY_SIZE,
                                   NULL,
                                   0,
                                   NULL,
                                   &fbh->dsize );
            msg = (ERRTYPE(ret) ? "ColAttribute failed" : "Invalid Handle");
            ret = check_error(h,ret,msg);
            EOI(ret);

            fbh->rlen = fbh->bufferSize = fbh->dsize+1;/* +1: STRING null terminator */
        }

        /* Limit buffer size based on LongReadLen for long column types */
        if( SQLTypeIsLong( fbh->dbtype ) )
        {
          unsigned int longReadLen = DBIc_LongReadLen( imp_sth );

          if( fbh->rlen > longReadLen )
          {
            if( SQL_LONGVARBINARY == fbh->dbtype ||
                SQL_BLOB == fbh->dbtype ||
                0 == longReadLen )
              fbh->rlen = fbh->bufferSize = longReadLen;
            else
              fbh->rlen = fbh->bufferSize = longReadLen+1; /* +1 for null terminator */
          }
        }

        /* Allocate output buffer */
        if( fbh->bufferSize > 0 )
          Newc( 1, fbh->buffer, fbh->bufferSize, SQLCHAR, void* );

        /* BIND */
        ret = SQLBindCol( imp_sth->phstmt,
                          i+1,
                          fbh->ftype,
                          fbh->buffer,
                          fbh->bufferSize,
                          &fbh->rlen );
        if (ret == SQL_SUCCESS_WITH_INFO ) {
            warn("BindCol error on %s: %d", fbh->cbuf);
        } else {
            msg = (ERRTYPE(ret) ? "BindCol failed" : "Invalid Handle");
            ret = check_error(h,ret,msg);
            EOI(ret);
        }

        if (DBIS->debug >= 2)
            fbh_dump(fbh, i);
    }
    return 1;
}

int
dbd_conn_opt(sth, opt, value)
    SV *sth;
    IV  opt;
    IV  value;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    char *msg;
    SQLINTEGER ret ;

    ret = SQLSetConnectOption(imp_dbh->hdbc,opt, value);
    msg = "SQLSetConnectOption failed";
    ret = check_error(sth,ret,msg);
    EOI(ret);

    return 1;
}

int
dbd_st_opts(sth, opt, value)
    SV *sth;
    IV    opt;
    IV    value;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    char *msg;
    SQLINTEGER ret ;

    ret = SQLSetStmtOption(imp_sth->phstmt,opt, value);
    msg = "SQLSetStmtOption failed";
    ret = check_error(sth,ret,msg);
    EOI(ret);

    return 1;
}

int
dbd_st_execute(sth)     /* error : <=(-2), ok row count : >=0, unknown count : (-1)     */
    SV *sth;
{
    D_imp_sth(sth);
    char *msg;
    SQLINTEGER ret;

    /* Reset input size and reallocate buffer if necessary for in/out
       parameters */
    if( imp_sth->bind_names && imp_sth->bHasInput )
    {
      HV *hv = imp_sth->bind_names;
      SV *sv;
      char *key;
      I32 retlen;
      phs_t *phs;
      STRLEN value_len;

      hv_iterinit( hv );
      while( ( sv = hv_iternextsv( hv, &key, &retlen ) ) != NULL )
      {
        if( SvOK( sv ) )
        {
          phs = (phs_t*)SvPVX( sv );

          if( NULL != phs->sv && /* is this parameter bound by reference? */
              SQL_PARAM_OUTPUT != phs->paramType ) /* is it in or in/out? */
          {
            if( SvOK( phs->sv ) )
            {
              SvPV( phs->sv, value_len );  /* Get input value length */
              if( value_len > (phs->bufferSize-1) )
                croak( "Error: Input value for parameter '%s' is bigger "
                       "than the maximum length specified", key );

              phs->indp = value_len;
              memcpy( phs->buffer, SvPVX( phs->sv ), value_len );
            }
            else
            {
              phs->indp = SQL_NULL_DATA;
            }
          }
        }
      }
    }

    /* describe and allocate storage for results        */
    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
        /* dbd_describe has already called check_error()        */
        return -2;
    }

    ret = SQLExecute(imp_sth->phstmt);
    msg = "SQLExecute failed";
    ret = check_error(sth,ret,msg);
    if (ret < 0)
        return(-2);

    if( imp_sth->bind_names && imp_sth->bHasOutput )
    {
      HV *hv = imp_sth->bind_names;
      SV *sv;
      char *key;
      I32 retlen;
      phs_t *phs;
      STRLEN value_len;

      hv_iterinit( hv );
      while( ( sv = hv_iternextsv( hv, &key, &retlen ) ) != NULL )
      {
        if( SvOK( sv ) )
        {
          phs = (phs_t*)SvPVX( sv );
          if( NULL != phs->sv && /* is this parameter bound by reference? */
              SQL_PARAM_INPUT != phs->paramType ) /* is it out or in/out? */
          {
            if( SQL_NULL_DATA == phs->indp )
              sv_setsv( phs->sv, &PL_sv_undef ); /* undefine variable */
            else if( SQL_NO_TOTAL == phs->indp )
            {
              sv_setsv( phs->sv, &PL_sv_undef ); /* undefine variable */
              warn( "Number of bytes available to return "
                    "cannot be determined for parameter '%s'", key );
            }
            else
            {
              sv_setpvn( phs->sv, phs->buffer, phs->indp );

              if( phs->indp > phs->bufferSize )
                warn( "Output buffer too small, data truncated "
                      "for parameter '%s'", key );
            }
          }
        }
      }
    }

    ret = SQLRowCount(imp_sth->phstmt, &imp_sth->RowCount);
    msg = "SQLRowCount failed";
    ret = check_error(sth, ret, msg);

    DBIc_ACTIVE_on(imp_sth);
    return imp_sth->RowCount;
}



AV *
dbd_st_fetch(sth)
    SV *    sth;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER num_fields;
    SQLINTEGER ChopBlanks;
    SQLINTEGER i,ret;
    AV *av;
    char *msg;
    imp_fbh_t *fbh;
    SV *sv;

    /* Check that execute() was executed sucessfuly. This also implies    */
    /* that dbd_describe() executed sucessfuly so the memory buffers    */
    /* are allocated and bound.                        */
    if ( !DBIc_ACTIVE(imp_sth) ) {
        check_error(sth, -3,"no statement executing");
        return Nullav;
    }

    ret = SQLFetch(imp_sth->phstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        if (ret != SQL_NO_DATA_FOUND) {    /* was not just end-of-fetch    */
            if (DBIS->debug >= 3)
                fprintf(DBILOGFP, "    dbd_st_fetch failed, rc=%d", ret);
            msg = (ERRTYPE(ret) ? "Fetch failed" : "Invalid Handle");
            check_error(sth,ret,msg);
        }
        dbd_st_finish(sth, imp_sth);
        return Nullav;
    }

    av = DBIS->get_fbav(imp_sth);
    num_fields = AvFILL(av)+1;

    if (DBIS->debug >= 3)
    fprintf(DBILOGFP, "    dbd_st_fetch %d fields\n", num_fields);

    ChopBlanks = DBIc_has( imp_sth, DBIcf_ChopBlanks );
    for( i = 0; i < num_fields; ++i )
    {
      fbh = &imp_sth->fbh[i];
      sv = AvARRAY(av)[i]; /* Note: we reuse the supplied SV    */

      if( fbh->rlen > -1 &&      /* normal case - column is not null */
          fbh->bufferSize > 0 )
      {
        int nullAdj = SQL_C_CHAR == fbh->ftype ? 1 : 0;

        if( fbh->rlen > ( fbh->bufferSize - nullAdj ) ) /* data has been truncated */
        {
          int longTruncOk = DBIc_has( imp_sth, DBIcf_LongTruncOk );
          char msg[200];

          sv_setpvn( sv, (char*)fbh->buffer, fbh->rlen );

          sprintf( msg,
                   "%s: Data in column %d has been truncated to %d bytes."
                   "  A maximum of %d bytes are available.",
                   longTruncOk ? "Warning" : "Error",
                   i,
                   fbh->bufferSize - nullAdj,
                   fbh->rlen );

          if( longTruncOk )
            warn( msg );
          else
            croak( msg );
        }
        else if( ChopBlanks && SQL_CHAR == fbh->dbtype )
          sv_setpvn( sv,
                     fbh->buffer,
                     GetTrimmedSpaceLen( fbh->buffer, fbh->rlen ) );
        else
          sv_setpvn( sv, (char*)fbh->buffer, fbh->rlen );
      }
      else                  /*  column contains a null value */
      {
        fbh->indp = fbh->rlen;
        fbh->rlen = 0;
        (void)SvOK_off(sv);
      }


      if( DBIS->debug >= 2 )
        fprintf(DBILOGFP, "\t%d: rc=%d '%s'\n", i, ret, SvPV(sv,na));
    }
    return av;
}

int
dbd_st_lob_read(sth, field, offset, len, destrv, destoffset)
    SV *sth;
    int field;
    long offset;
    long len;
    SV *destrv;
    long destoffset;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER retl;
    SV *bufsv;
    SQLINTEGER rtval;
    char *msg;
    imp_fbh_t *fbh;
    int cbNullSize;  /* 1 if null terminated, 0 otherwise */

    if( field < 1 || field > DBIc_NUM_FIELDS(imp_sth) )
      croak( "Error: Column %d is out of range", field );

    fbh = &imp_sth->fbh[field-1];

    if( SQLTypeIsGraphic( fbh->dbtype ) )
    {
       cbNullSize = 0;          /* graphic data is not null terminated */
       if( len%2 == 1 )
         len -= 1; /* graphic column data requires an even buffer size */
    }
    else if( SQLTypeIsBinary( fbh->dbtype ) )
    {
       cbNullSize = 0;           /* binary data is not null terminated */
    }
    else
    {
       cbNullSize = 1;
    }

    bufsv = SvRV(destrv);
    if( SvREADONLY( bufsv ) )
       croak( "Error: Modification of a read-only value attempted" );
    if( !SvOK( bufsv) )
       sv_setpv( bufsv, "" ); /* initialize undefined variable */
    SvGROW( bufsv, destoffset + len + cbNullSize );

    rtval = SQLGetData( imp_sth->phstmt,
                        field,
                        fbh->ftype,
                        SvPVX(bufsv) + destoffset,
                        len + cbNullSize,
                        &retl );

    if (rtval == SQL_SUCCESS_WITH_INFO)      /* XXX should check for 01004 */
       retl = len;

    if (retl == SQL_NULL_DATA)      /* field is null    */
    {
       (void)SvOK_off(bufsv);
       return 1;
    }

    msg = (ERRTYPE(rtval) ? "GetData failed to read LOB":"Invalid Handle");
    rtval = check_error(sth,rtval,msg);
    EOI(rtval);

    SvCUR_set(bufsv, destoffset+retl );
    *SvEND(bufsv) = '\0'; /* consistent with perl sv_setpvn etc    */

    return 1;
}


int
dbd_st_rows(sth, imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    return imp_sth->RowCount;
}


int
dbd_st_finish(sth,imp_sth)
    SV *sth;
    imp_sth_t *imp_sth;
{
    D_imp_dbh_from_sth;
    SQLINTEGER ret;
    char *msg;

    /* Cancel further fetches from this cursor.  We don't        */
    /* close the cursor (SQL_DROP) 'til DESTROY (dbd_st_destroy).*/
    /* The application may call execute(...) again on the same   */
    /* statement handle.                                         */

    if (DBIc_ACTIVE(imp_sth) ) {
        ret = SQLFreeStmt(imp_sth->phstmt,SQL_CLOSE);
        msg = (ERRTYPE(ret) ? "SQLFreeStmt failed" : "Invalid Handle");
        ret = check_error(sth,ret,msg);
        EOI(ret);
    }
    DBIc_ACTIVE_off(imp_sth);
    return 1;
}


int
dbd_st_destroy(sth)
    SV *sth;
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLINTEGER i;

    /* Free off contents of imp_sth    */

    for(i=0; i < DBIc_NUM_FIELDS(imp_sth); ++i) {
        imp_fbh_t *fbh = &imp_sth->fbh[i];
        Safefree( fbh->buffer );
    }
    Safefree(imp_sth->fbh);
    Safefree(imp_sth->fbh_cbuf);
    Safefree(imp_sth->statement);

    if (imp_sth->bind_names)
    {
      HV *hv = imp_sth->bind_names;
      SV *sv;
      char *key;
      I32 retlen;
      phs_t *phs;

      hv_iterinit(hv);
      while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL )
      {
        if (sv != &sv_undef)
        {
          phs = (phs_t*)SvPVX(sv);
          SvREFCNT_dec( phs->sv );
          if( phs->buffer != NULL && phs->bufferSize > 0 )
             Safefree( phs->buffer );
        }
      }
      sv_free((SV*)imp_sth->bind_names);
    }
/*
 * Chet Murthy's patch for DB2 heap overflow problems
*/
    /* Check if an explicit disconnect() or global destruction has      */
    /* disconnected us from the database before attempting to drop      */
    /* the CLI statement handle.  If we don't do this, a coredump can   */
    /* potentially occur.                                               */
    if (DBIc_ACTIVE(imp_dbh)) {
        i = SQLFreeStmt (imp_sth->phstmt, SQL_DROP);
        if (i != SQL_SUCCESS && i != SQL_INVALID_HANDLE) {
            i = check_error(NULL,i, "Statement destruction error");
        }
    }
/* End Chet */

    DBIc_IMPSET_off(imp_sth);        /*  let DBI know we've done it    */
    return 1;
}


int
dbd_st_STORE(sth, keysv, valuesv)
    SV *sth;
    SV *keysv;
    SV *valuesv;
{
    D_imp_sth(sth);
    STRLEN kl;
    char *key = SvPV(keysv,kl);
    SV *cachesv = NULL;
    int on = SvTRUE(valuesv);

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
    char *key = SvPV(keysv,kl);
    int i;
    SV *retsv = NULL;
    char cursor_name[256];
    SQLSMALLINT cursor_name_len;
    SQLINTEGER ret;
    char *msg;

    /* Default to caching results for DBI dispatch quick_FETCH  */
    int cacheit = TRUE;

    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
        /* dbd_describe has already called check_error()        */
        /* We can't return Nullsv here because the xs code will */
        /* then just pass the attribute name to DBI for FETCH.  */
        croak("Describe failed during %s->FETCH(%s)",
               SvPV(sth,na), key);
    }

    i = DBIc_NUM_FIELDS(imp_sth);

    if (kl==7 && strEQ(key, "lengths")) {
        AV *av = newAV();
        retsv = newRV((SV*)av);
        while(--i >= 0)
            av_store(av, i, newSViv((IV)imp_sth->fbh[i].dsize));
    } else if (kl==5 && strEQ(key, "types")) {
        AV *av = newAV();
        retsv = newRV((SV*)av);
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->fbh[i].dbtype));

    } else if (kl==13 && strEQ(key, "NUM_OF_PARAMS")) {
        HV *bn = imp_sth->bind_names;
        retsv = newSViv( (bn) ? HvKEYS(bn) : 0 );

    } else if (kl==4 && strEQ(key, "NAME")) {
        AV *av = newAV();
        retsv = newRV((SV*)av);
        while(--i >= 0)
            av_store(av, i, newSVpv((char *)imp_sth->fbh[i].cbuf,0));

    } else if (kl==8 && strEQ(key, "NULLABLE")) {
        AV *av = newAV();
        retsv = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i,
                     (imp_sth->fbh[i].nullok == 1) ? &sv_yes : &sv_no);

    } else if (kl==10 && strEQ(key, "CursorName")) {
        ret = SQLGetCursorName(imp_sth->phstmt, (SQLCHAR *)cursor_name,
                               sizeof(cursor_name), &cursor_name_len);
        msg = "SQLGetCursorName failed";
        ret = check_error(sth, ret, msg);
        if (ret < 0)
            return Nullsv;
        else
            retsv = newSVpv(cursor_name, cursor_name_len);

    } else if (kl==4 && strEQ(key, "TYPE")) {
        AV *av = newAV();
        retsv = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->fbh[i].dbtype));

    } else if (kl==9 && strEQ(key, "PRECISION")) {
        AV *av = newAV();
        retsv = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->fbh[i].prec));

    } else if (kl==5 && strEQ(key, "SCALE")) {
        AV *av = newAV();
        retsv = newRV(sv_2mortal((SV*)av));
        while(--i >= 0)
            av_store(av, i, newSViv(imp_sth->fbh[i].scale));

    } else {
        return Nullsv;
    }

    if (cacheit) { /* cache for next time (via DBI quick_FETCH)    */
         SV **svp = hv_fetch((HV*)SvRV(sth), key, kl, 1);
         sv_free(*svp);
         *svp = retsv;
        (void)SvREFCNT_inc(retsv);    /* so sv_2mortal won't free it    */
    }
    return sv_2mortal(retsv);
}
