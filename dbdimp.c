/*
   engn/perldb2/dbdimp.c, engn_perldb2, db2_v82fp9, 1.10 04/09/13 17:14:24

   Copyright (c) 1995-2004 International Business Machines Corp.
*/

#include <stdio.h>
#include "DB2.h"
#ifndef AS400
#include "sqlenv.h"                                              
#endif

#define EOI(x)  if (x < 0 || x == SQL_NO_DATA) return (0)

/* These did not exist in the first release of DB2 v5.2 */
#ifndef SQL_ATTR_CONNECT_NODE                                   
 #define SQL_ATTR_CONNECT_NODE 1290
#endif

#ifndef SQL_ATTR_DB2_SQLERRP
 #define SQL_ATTR_DB2_SQLERRP  2451
#endif                                                          

#ifndef SQL_ATTR_OPTIMIZE_FOR_NROWS                             
 #define SQL_ATTR_OPTIMIZE_FOR_NROWS 2450
#endif

#ifndef SQL_ATTR_QUERY_OPTIMIZATION_LEVEL
 #define SQL_ATTR_QUERY_OPTIMIZATION_LEVEL 1293
#endif                                                          

DBISTATE_DECLARE;

void dbd_init( dbistate_t *dbistate )                            
{
    DBIS = dbistate;
}

static SQLRETURN do_error( SV *h,                                
                           SQLRETURN rc,                         
                           SQLSMALLINT handleType,               
                           SQLHANDLE handle,                     
                           char *what )
{
    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);
    SV *state = DBIc_STATE(imp_xxh);
    SQLINTEGER sqlcode;
    SQLCHAR sqlstate[6];
    SQLCHAR msgBuffer[SQL_MAX_MESSAGE_LENGTH+1];                 
    char *msg = NULL;                                            

    if( SQL_SUCCESS == rc )                                      
      return rc;

    if( SQL_NO_DATA == rc )                                     
    {
      sv_setpv( state, SQL_NODATA_EXCEPTION );
      return rc;
    }                                                           

    if( SQL_SUCCESS_WITH_INFO == rc && DBIS->debug < 3 )         
      return SQL_SUCCESS;                                        

    if( handle != SQL_NULL_HANDLE )                             
    {
      SQLRETURN rc;
      SQLSMALLINT length;

      rc = SQLGetDiagRec( handleType,
                          handle,
                          (SQLSMALLINT)1,
                          sqlstate,
                          &sqlcode,
                          msgBuffer,
                          (SQLSMALLINT)sizeof( msgBuffer ),
                          &length );

      if( SQL_SUCCESS == rc ||
          SQL_SUCCESS_WITH_INFO == rc )
        msg = (char*)msgBuffer;
    }

    if( NULL == msg )
    {
      sqlcode = rc;
      strcpy( (char*)sqlstate, "00000" );                        
      msg = what ? what : "";
    }                                                           

    sv_setiv(DBIc_ERR(imp_xxh), (IV)sqlcode);
    sv_setpv(errstr, (char *)msg);
    sv_setpv(state,(char *)sqlstate);
    DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr);
    if (DBIS->debug >= 2)                                        
      PerlIO_printf( DBILOGFP,                                   
                     "%s error %d recorded: %s\n",
                     what, rc, SvPV(errstr,na) );

    return( rc == SQL_SUCCESS_WITH_INFO ? SQL_SUCCESS : rc );    
}

static SQLRETURN check_error( SV *h,                            
                              SQLRETURN rc,
                              char *what )
{
    D_imp_xxh(h);
    SQLSMALLINT handleType;
    SQLHANDLE handle = SQL_NULL_HANDLE;

    if( SQL_SUCCESS == rc )                                      
      return rc;

    if( SQL_NO_DATA == rc )                                     
    {
      sv_setpv( DBIc_STATE( imp_xxh ), SQL_NODATA_EXCEPTION );
      return rc;
    }                                                           

    if( SQL_SUCCESS_WITH_INFO == rc && DBIS->debug < 3 )
      return SQL_SUCCESS;

    if( SQL_ERROR == rc || SQL_SUCCESS_WITH_INFO == rc )
    {
      imp_sth_t *imp_sth;
      imp_dbh_t *imp_dbh;
      imp_drh_t *imp_drh;

      while( SQL_NULL_HANDLE == handle && imp_xxh )
      {
        switch( DBIc_TYPE( imp_xxh ) )
        {
          case DBIt_ST:
            imp_sth = (imp_sth_t *)(imp_xxh);
            if( imp_sth->phstmt )
            {
              handleType = SQL_HANDLE_STMT;
              handle = imp_sth->phstmt;
            }
            else
            {
              imp_xxh = (imp_xxh_t *)(DBIc_PARENT_COM(imp_sth));
            }
            break;
          case DBIt_DB:
            imp_dbh = (imp_dbh_t *)(imp_xxh);
            if( imp_dbh->hdbc )
            {
              handleType = SQL_HANDLE_DBC;
              handle = imp_dbh->hdbc;
            }
            else
            {
              imp_xxh = (imp_xxh_t *)(DBIc_PARENT_COM(imp_dbh));
            }
            break;
          case DBIt_DR:
            imp_drh = (imp_drh_t *)(imp_xxh);
            if( imp_drh->henv )
            {
              handleType = SQL_HANDLE_ENV;
              handle = imp_dbh->henv;
            }
            else
            {
              imp_xxh = NULL;
            }
            break;
        }
      }
    }

    rc = do_error( h,
                   rc,
                   handleType,
                   handle,
                   what );

    return rc;
}                                                               

static void fbh_dump( imp_fbh_t *fbh,                            
                      int i )
{
    PerlIO_printf( DBILOGFP, "fbh %d: '%s' %s, ",                
        i, fbh->cbuf, (fbh->nullok) ? "NULLable" : "" );
    PerlIO_printf( DBILOGFP, "type %d,  %ld, dsize %ld, p%d s%d\n", 
        fbh->dbtype, (long)fbh->dsize, fbh->prec, fbh->scale );  
    PerlIO_printf( DBILOGFP, "   out: ftype %d, indp %d, bufl %d, rlen %d\n", 
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
  #ifdef AS400
    if(SQL_BLOB == SQLType)
      return 0;
  #endif

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
   } while( i < len && charLen > 0 );                            


   return trimmedLen;
}                                                               

/* ================================================================== */

#ifndef AS400
AV *                                                            
dbd_data_sources( SV *drh )
{
  AV *ds = newAV();
  unsigned short dbHandle = 0;
  unsigned short dbCount;
  struct sqlca sqlca;
  struct sqledinfo *dbBuffer;
  const int prefixLen = 8;  /* length of 'dbi:DB2:' */
  char buffer[ 8 + sizeof( dbBuffer->alias ) + 1 ] = "dbi:DB2:";
  char *const pAlias = buffer + prefixLen;
  int cbLen;
  SQLRETURN ret;

  sqledosd( NULL, &dbHandle, &dbCount, &sqlca );

  /* Convert following two return codes to SQL_NO_DATA so the state */
  /* gets set correctly in do_error */
  if( SQLE_RC_NODBDIR == sqlca.sqlcode ||
      SQLE_RC_NODENTRY == sqlca.sqlcode )
    sqlca.sqlcode = SQL_NO_DATA;

  ret = do_error( drh,
                  (SQLRETURN) sqlca.sqlcode,
                  0,
                  SQL_NULL_HANDLE,
                  "sqledosd failed" );
  if( SQL_SUCCESS != ret )
    goto exit;

  for( ; dbCount > 0; dbCount-- )
  {
    sqledgne( dbHandle, &dbBuffer, &sqlca );
    ret = do_error( drh,
                    (SQLRETURN) sqlca.sqlcode,
                    0,
                    SQL_NULL_HANDLE,
                    "sqledgne failed" );
    if( SQL_SUCCESS != ret )
      goto exit;

    /* alias is blank padded, determine actual length of alias */
    for( cbLen = sizeof( dbBuffer->alias );
         cbLen > 0 && dbBuffer->alias[cbLen-1] == ' ';
         cbLen-- );

    strncpy( pAlias,
             dbBuffer->alias,
             cbLen );
    av_push( ds, newSVpv( buffer, prefixLen + cbLen ) );
  }

exit:
  if( 0 != dbHandle )
    sqledcls( dbHandle, &sqlca );

  return ds;
}                                                               
#endif

/* ================================================================== */

static int dbd_db_connect( SV *dbh,
                           imp_dbh_t *imp_dbh,
                           SQLCHAR *dbname,
                           SQLCHAR *uid,
                           SQLCHAR *pwd,
                           SV *attr )                            
{
  D_imp_drh_from_dbh;
  SQLRETURN ret;                                                 

  imp_dbh->hdbc = SQL_NULL_HDBC;                                 

  ret = SQLAllocHandle(SQL_HANDLE_DBC, imp_drh->henv,
                       &imp_dbh->hdbc);                           
  ret = check_error( dbh, ret, "DB handle allocation failed" );
  if( SQL_SUCCESS != ret )
    goto exit;                                                   

  if (DBIS->debug >= 2)                                          
    PerlIO_printf( DBILOGFP,                                     
                   "connect '%s', '%s', '%s'", dbname, uid, pwd );

  /*
   * The SQL_ATTR_CONNECT_NODE attribute must be set prior
   * to establishing the connection:
   */
  if( SvROK( attr ) && SvTYPE( SvRV( attr ) ) == SVt_PVHV )     
  {
    HV *attrh = (HV*)SvRV( attr );
    SV **pval;

    pval = hv_fetch( attrh, "db2_connect_node", 16, 0 );
    if( NULL != pval )
    {
      ret = SQLSetConnectAttr( imp_dbh->hdbc,
                               SQL_ATTR_CONNECT_NODE,
                               (SQLPOINTER)SvIV( *pval ),
                               0 );
      ret = check_error( dbh, ret, "Set connect node failed" );
      if( SQL_SUCCESS != ret )
        goto exit;
    }
  }                                                             

  ret = SQLConnect(imp_dbh->hdbc,dbname,SQL_NTS,uid,SQL_NTS,pwd,SQL_NTS);
  ret = check_error( dbh, ret, "Connect failed" );
  if( SQL_SUCCESS != ret )
    goto exit;                                                   

  /* Set default value for LongReadLen */
  DBIc_LongReadLen( imp_dbh ) = 32700;                           


exit:                                                           
  if( SQL_SUCCESS != ret )
  {
    if( SQL_NULL_HDBC != imp_dbh->hdbc )
      SQLFreeHandle( SQL_HANDLE_DBC, imp_dbh->hdbc );

    if( 0 == imp_drh->connects )
    {
      if( NULL != imp_drh->svNUM_OF_FIELDS )                    
      {
        SvREFCNT_dec( imp_drh->svNUM_OF_FIELDS );
        imp_drh->svNUM_OF_FIELDS = NULL;
      }                                                         
      SQLFreeHandle( SQL_HANDLE_ENV, imp_drh->henv );
      imp_drh->henv = SQL_NULL_HENV;
    }
  }

  return ret;                                                   
}


int dbd_db_login2( SV *dbh,
                   imp_dbh_t *imp_dbh,
                   char *dbname,
                   char *uid,
                   char *pwd,
                   SV *attr )                                    
{
    D_imp_drh_from_dbh;
    SQLRETURN ret;                                               

    if (! imp_drh->connects) {
        ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,     
                             &imp_drh->henv);                     
        ret = check_error( dbh,
                           ret,
                           SQL_NULL_HENV == imp_drh->henv
                ? "Total Environment allocation failure!  "      
                  "Did you set up your DB2 client environment?"  
                : "Environment allocation failed" );
        EOI(ret);

#ifndef AS400
                                                                 
        /* If an application is run as an ODBC application, the         */
        /* SQL_ATTR_ODBC_VERSION environment attribute must be set;     */
        /* otherwise, an error will be returned when an attempt is      */
        /* made to allocate a connection handle.                        */
        ret = SQLSetEnvAttr( imp_drh->henv, SQL_ATTR_ODBC_VERSION,
                             (SQLPOINTER) SQL_OV_ODBC3, 0 );
        ret = check_error( dbh, ret, "SQLSetEnvAttr failed" );
        EOI(ret);                                                
#endif
    }
    imp_dbh->henv = imp_drh->henv;
    ret = dbd_db_connect( dbh,
                          imp_dbh,
                          (SQLCHAR*)dbname,                      
                          (SQLCHAR*)uid,                         
                          (SQLCHAR*)pwd,                         
                          attr );                                
    EOI(ret);
    imp_drh->connects++;

    DBIc_IMPSET_on(imp_dbh);    /* imp_dbh set up now            */
    DBIc_ACTIVE_on(imp_dbh);    /* call disconnect before freeing    */
    return 1;
}


int dbd_db_do( SV *dbh,                                          
               char *statement ) /* error : <=(-2), ok row count : >=0, unknown count : (-1)   */
{
    D_imp_dbh(dbh);
    SQLRETURN ret;                                               
    SQLINTEGER rows;                                             
    SQLHSTMT stmt;

    ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &stmt );
    ret = check_error( dbh, ret, "Statement allocation error" );
    if (ret < 0)                                                  
        return(-2);                                               

    ret = SQLExecDirect(stmt, (SQLCHAR *)statement, SQL_NTS);
    ret = do_error( dbh,                                         
                    ret,
                    SQL_HANDLE_STMT,                             
                    stmt,                                        
                    "Execute immediate failed" );
    if (ret < 0)
        rows = -2;
    else {
        ret = SQLRowCount(stmt, &rows);
        ret = do_error( dbh,                                     
                        ret,
                        SQL_HANDLE_STMT,                         
                        stmt,                                    
                        "SQLRowCount failed" );
        if (ret < 0)
            rows = -1;
    }

    ret = SQLFreeHandle( SQL_HANDLE_STMT, stmt );
    (void) do_error( dbh,                                        
                     ret,
                     SQL_HANDLE_STMT,                            
                     stmt,                                       
                     "Statement destruction error" );

    return (int)rows;                                            
}


int dbd_db_ping( SV *dbh )                                      
{
    D_imp_dbh(dbh);
    SQLRETURN ret;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    const char *pSQL = "values 1";

    if( !DBIc_ACTIVE( imp_dbh ) )
      return 0;

    if( '\0' == imp_dbh->sqlerrp[0] )
    {
      SQLGetConnectAttr( imp_dbh->hdbc,
                         SQL_ATTR_DB2_SQLERRP,
                         (SQLPOINTER)imp_dbh->sqlerrp,
                         sizeof( imp_dbh->sqlerrp ),
                         NULL );
    }

    if( strncmp( imp_dbh->sqlerrp, "SQL", 3 ) == 0 )      /* UNO */
    {
      /* Do nothing, use the default statement */
    }
    else if( strncmp( imp_dbh->sqlerrp, "DSN", 3 ) == 0 ) /* MVS */
    {
      pSQL = "select 1 from sysibm.sysdummy1";
    }
    else if( strncmp( imp_dbh->sqlerrp, "QSQ", 3 ) == 0 ) /* AS/400 */
    {
      pSQL = "select 1 from qsys2.qsqptabl";
    }
    else if( strncmp( imp_dbh->sqlerrp, "ARI", 3 ) == 0 ) /* VM */
    {
      pSQL = "select 1 from system.sysoptions";
    }
    else
    {
      /* Do nothing, use the default statement */
    }

    ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &stmt );
    ret = do_error( dbh,
                    ret,
                    (SQLSMALLINT)(
                    stmt == SQL_NULL_HSTMT ? SQL_HANDLE_DBC
                                           : SQL_HANDLE_STMT),
                    (SQLHANDLE)(
                    stmt == SQL_NULL_HSTMT ? imp_dbh->hdbc
                                           : stmt),
                    "dbd_db_ping: Statement allocation error" );
    if( SQL_SUCCESS != ret )
       goto exit;

#ifndef AS400
    ret = SQLSetStmtAttr( stmt,
                          SQL_ATTR_DEFERRED_PREPARE,
                          SQL_DEFERRED_PREPARE_OFF,
                          0 );
    ret = do_error( dbh,
                    ret,
                    SQL_HANDLE_STMT,
                    stmt,
                    "dbd_db_ping: Error turning off deferred prepare" );
    if( SQL_SUCCESS != ret )
       goto exit;
#endif

    ret = SQLPrepare( stmt, (SQLCHAR*)pSQL, SQL_NTS );
    ret = do_error( dbh,
                    ret,
                    SQL_HANDLE_STMT,
                    stmt,
                    "dbd_db_ping: Error preparing statement" );

exit:
    if( stmt )
      SQLFreeHandle( SQL_HANDLE_STMT, stmt );

    /* If any error occured, check the state to determine if the cause */
    /* is a broken connection.                                         */
    if( SQL_SUCCESS != ret )
    {
      STRLEN len;
      char *pState = SvPV( DBIc_STATE(imp_dbh), len );

      if( pState && len >= 5 &&
          ( strncmp( pState, "08", 2 ) == 0 ||
            strncmp( pState, "40003", 5 ) == 0 ) )
      {
        /* ping should not throw an error when it detects a dead  */
        /* connection so reset error code and message but keep    */
        /* connection state                                       */
        sv_setsv( DBIc_ERRSTR(imp_dbh), &sv_undef );
        sv_setsv( DBIc_ERR(imp_dbh), &sv_undef );
        return 0; /* Connection is dead */
      }
    }

    return 1; /* Connection is still alive */
}                                                               


int dbd_db_commit( SV *dbh,                                      
                   imp_dbh_t *imp_dbh )
{
    SQLRETURN ret;                                               

    ret = SQLTransact(imp_dbh->henv,imp_dbh->hdbc,SQL_COMMIT);
    ret = check_error( dbh, ret, "Commit failed" );
    EOI(ret);
    return 1;
}


int dbd_db_rollback( SV *dbh,                                    
                     imp_dbh_t *imp_dbh )
{
    SQLRETURN ret;                                               

    ret = SQLTransact(imp_dbh->henv,imp_dbh->hdbc,SQL_ROLLBACK);
    ret = check_error( dbh, ret, "Rollback failed" );
    EOI(ret);
    return 1;
}


int dbd_db_disconnect( SV *dbh,                                  
                       imp_dbh_t *imp_dbh )
{
    D_imp_drh_from_dbh;
    SQLRETURN ret;                                               

    ret = SQLDisconnect(imp_dbh->hdbc);
    ret = check_error( dbh, ret, "Disconnect failed" );
    EOI(ret);
                                                                 
    /* Only turn off the ACTIVE attribute of the database handle        */
    /* if SQLDisconnect() was successful.  If it wasn't successful,     */
    /* we still have a connection!                                      */

    DBIc_ACTIVE_off(imp_dbh);                                    

    ret = SQLFreeHandle( SQL_HANDLE_DBC, imp_dbh->hdbc );        
    ret = check_error( dbh, ret, "Free connect failed" );
    EOI(ret);

    imp_dbh->hdbc = SQL_NULL_HDBC;
    imp_drh->connects--;
    if (imp_drh->connects == 0)
    {
      if( NULL != imp_drh->svNUM_OF_FIELDS )                    
      {
        SvREFCNT_dec( imp_drh->svNUM_OF_FIELDS );
        imp_drh->svNUM_OF_FIELDS = NULL;
      }                                                         
      ret = SQLFreeHandle( SQL_HANDLE_ENV, imp_drh->henv );      
      ret = check_error( dbh, ret, "Free henv failed" );
      EOI(ret);
      imp_drh->henv = SQL_NULL_HENV;                             
    }

    /* We don't free imp_dbh since a reference still exists    */
    /* The DESTROY method is the only one to 'free' memory.    */
    /* Note that statement objects may still exists for this dbh!    */
    return 1;
}


void dbd_db_destroy( SV *dbh,                                    
                     imp_dbh_t *imp_dbh )
{
    if (DBIc_ACTIVE(imp_dbh))
        dbd_db_disconnect(dbh,imp_dbh);
    /* Nothing in imp_dbh to be freed    */
    DBIc_IMPSET_off(imp_dbh);
}


static SQLINTEGER getConnectAttr( char *key,                    
                                  STRLEN keylen )
{
  /*
   * The following DB2 CLI connection attributes are not supported
   *
   *   SQL_ATTR_ASYNC_ENABLE        Doesn't make sense for DBD::DB2
   *   SQL_ATTR_AUTO_IPD            Doesn't make sense for DBD::DB2
   *   SQL_ATTR_CONNECTION_DEAD     not reliable, $dbh->ping is better
   *   SQL_ATTR_CONNECTTYPE         2-phase commit not supported
   *   SQL_ATTR_CONN_CONTEXT        Doesn't make sense for DBD::DB2
   *   SQL_ATTR_ENLIST_IN_DTC       Doesn't make sense for DBD::DB2
   *   SQL_ATTR_LOGIN_TIMEOUT       Not supported by DB2 CLI
   *   SQL_ATTR_MAXCONN             For NetBIOS
   *   SQL_ATTR_OPTIMIZE_SQLCOLUMNS
   *   SQL_ATTR_SYNC_POINT          2-phase commit not supported
   *   SQL_ATTR_TRANSLATE_LIB       Not supported by DB2 CLI
   *   SQL_ATTR_TRANSLATE_OPTION    Not supported by DB2 CLI
   *   SQL_ATTR_WCHARTYPE           Doesn't make sense for DBD::DB2
   *
   */

  /* For better performance, the keys are sorted by length */
  switch( keylen )
  {
    case 10:
      if(      strEQ( key, "AutoCommit" ) )
        return SQL_ATTR_AUTOCOMMIT;
      return -1;                                                 

    case 11:
      if(      strEQ( key, "db2_sqlerrp" ) )
        return SQL_ATTR_DB2_SQLERRP;
      return -1;                                                 

#ifndef AS400
    case 13:
      if(      strEQ( key, "db2_clischema" ) )
        return SQL_ATTR_CLISCHEMA;
      return -1;                                                 

    case 14:
      if(      strEQ( key, "db2_db2explain" ) )
        return SQL_ATTR_DB2EXPLAIN;
      else if( strEQ( key, "db2_quiet_mode" ) )
        return SQL_ATTR_QUIET_MODE;
      return -1;                                                 
#endif

    case 15:
      if(      strEQ( key, "db2_access_mode" ) )
        return SQL_ATTR_ACCESS_MODE;
#ifndef AS400
      else if( strEQ( key, "db2_db2estimate" ) )
        return SQL_ATTR_DB2ESTIMATE;
      else if( strEQ( key, "db2_info_userid" ) )
        return SQL_ATTR_INFO_USERID;
#endif
      return -1;                                                 

    case 16:
      if(      strEQ( key, "db2_connect_node" ) )
        return SQL_ATTR_CONNECT_NODE;
#ifndef AS400
      else if( strEQ( key, "db2_info_acctstr" ) )
        return SQL_ATTR_INFO_ACCTSTR;
#endif
      return -1;                                                 

#ifndef AS400
    case 17:
      if(      strEQ( key, "db2_info_applname" ) )
        return SQL_ATTR_INFO_APPLNAME;
      else if( strEQ( key, "db2_txn_isolation" ) )
        return SQL_ATTR_TXN_ISOLATION;
      return -1;                                                 

    case 18:
      if(      strEQ( key, "db2_close_behavior" ) )
        return SQL_ATTR_CLOSE_BEHAVIOR;
      else if( strEQ( key, "db2_current_schema" ) )
        return SQL_ATTR_CURRENT_SCHEMA;
      return -1;                                                 

    case 19:
      if( strEQ( key, "db2_info_wrkstnname" ) )
        return SQL_ATTR_INFO_WRKSTNNAME;
      else if( strEQ( key, "db2_longdata_compat" ) )
        return SQL_ATTR_LONGDATA_COMPAT;
      return -1;                                                 
#endif

    default:
      return -1;                                                 
  }
}                                                               


int dbd_db_STORE_attrib( SV *dbh,                               
                         imp_dbh_t *imp_dbh,
                         SV *keysv,
                         SV *valuesv )
{
  STRLEN kl;
  char *key = SvPV( keysv, kl );
  SQLINTEGER Attribute = getConnectAttr( key, kl );
  SQLRETURN ret;
#ifndef AS400
  SQLPOINTER ValuePtr = 0;
#else
  SQLPOINTER ValuePtr = 8;
#endif
  SQLINTEGER StringLength = 0;
  char msg[128]; /* buffer for error messages */
#ifdef AS400
  SQLINTEGER param;
#endif

  if( Attribute < 0 ) /* Don't know what this attribute is */    
    return FALSE;

  switch( Attribute )
  {
    /* Booleans */
#ifndef AS400
    case SQL_ATTR_AUTOCOMMIT:
    case SQL_ATTR_LONGDATA_COMPAT:
      if( SvTRUE( valuesv ) )
        ValuePtr = (SQLPOINTER)1;
      break;
#else
    case SQL_ATTR_AUTOCOMMIT:
      param = SQL_AUTOCOMMIT_ON;
      if( SvTRUE( valuesv ) )
        ValuePtr = (SQLPOINTER)&param;
      break;
#endif

    /* Strings */
#ifndef AS400
    case SQL_ATTR_CLISCHEMA:
    case SQL_ATTR_CURRENT_SCHEMA:
    case SQL_ATTR_INFO_ACCTSTR:
    case SQL_ATTR_INFO_APPLNAME:
    case SQL_ATTR_INFO_USERID:
    case SQL_ATTR_INFO_WRKSTNNAME:
      if( SvOK( valuesv ) )
      {
        STRLEN vl;
        ValuePtr = (SQLPOINTER)SvPV( valuesv, vl );
        StringLength = (SQLINTEGER)vl;
      }
      break;
#endif

    /* Integers */
    case SQL_ATTR_ACCESS_MODE:
#ifndef AS400
    case SQL_ATTR_CLOSE_BEHAVIOR:
#endif
    case SQL_ATTR_CONNECT_NODE:
#ifndef AS400
    case SQL_ATTR_DB2ESTIMATE:
    case SQL_ATTR_DB2EXPLAIN:
    case SQL_ATTR_QUIET_MODE:
    case SQL_ATTR_TXN_ISOLATION:
#endif
      if( SvIOK( valuesv ) )
      {
        ValuePtr = (SQLPOINTER)SvIV( valuesv );
      }
      else if( SvOK( valuesv ) )
      {
        /* Value is not an integer, return error */
        sprintf( msg,
                 "Invalid value for connection attribute %s, expecting integer",
                 key );
        do_error( dbh, -1, 0, SQL_NULL_HANDLE, msg );
        return FALSE;
      }
#ifndef AS400
      else /* Undefined, Set to default, most are 0 or NULL */
      {
        if( SQL_ATTR_TXN_ISOLATION == Attribute )
          ValuePtr = (SQLPOINTER)SQL_TXN_READ_COMMITTED;
      }
#endif
      break;

    default:
      return FALSE;
  }


  ret = SQLSetConnectAttr( imp_dbh->hdbc,
                           Attribute,
                           ValuePtr,
                           StringLength );
  if( SQL_SUCCESS != ret )
  {
    sprintf( msg, "Error setting %s connection attribute", key );
    check_error( dbh, ret, msg );
    return FALSE;
  }

  if( SQL_ATTR_AUTOCOMMIT == Attribute )
  {
    DBIc_set( imp_dbh, DBIcf_AutoCommit, SvTRUE( valuesv ) );
  }

  return TRUE;
}                                                               


SV *dbd_db_FETCH_attrib( SV *dbh,                               
                         imp_dbh_t *imp_dbh,
                         SV *keysv )
{
  STRLEN kl;
  char *key = SvPV( keysv, kl );
  SQLINTEGER Attribute = getConnectAttr( key, kl );
  SV *retsv = NULL;
  SQLRETURN ret;
  char buffer[128]; /* should be big enough for any attribute value */
  SQLPOINTER ValuePtr = (SQLPOINTER)buffer;
  SQLINTEGER BufferLength = sizeof( buffer );
  SQLINTEGER StringLength;

  if( Attribute < 0 ) /* Don't know what this attribute is */    
    return NULL;

  ret = SQLGetConnectAttr( imp_dbh->hdbc,
                           Attribute,
                           ValuePtr,
                           BufferLength,
                           &StringLength );
  if( SQL_SUCCESS_WITH_INFO == ret &&
      (StringLength + 1) > BufferLength )
  {
    /* local buffer isn't big enough, allocate one */
    BufferLength = StringLength + 1;
    Newc( 1, ValuePtr, BufferLength, char, SQLPOINTER );
    ret = SQLGetConnectAttr( imp_dbh->hdbc,
                             Attribute,
                             ValuePtr,
                             BufferLength,
                             &StringLength );
  }

  ret = check_error( dbh, ret, "Error retrieving connection attribute" );
  if( SQL_SUCCESS == ret )
  {
    switch( Attribute )
    {
      /* Booleans */
      case SQL_ATTR_AUTOCOMMIT:
#ifndef AS400
      case SQL_ATTR_LONGDATA_COMPAT:
#endif
        if( *(SQLINTEGER*)ValuePtr )
          retsv = &sv_yes;
        else
          retsv = &sv_no;
        break;

      /* Strings */
#ifndef AS400
      case SQL_ATTR_CURRENT_SCHEMA:
        /* Due to a DB2 CLI bug, a StringLength of 1 is returned */
        /* for current schema when it should return 0.  However, */
        /* the first byte is correctly set to 0 so we need to    */
        /* check that to distinguish an empty string from a 1    */
        /* byte string.                                          */
        if( 1 == StringLength && '\0' == ((char*)ValuePtr)[0] )
          StringLength = 0;
        /* don't break, fall through to regular string processing */
      case SQL_ATTR_CLISCHEMA:
#endif
      case SQL_ATTR_DB2_SQLERRP:
#ifndef AS400
      case SQL_ATTR_INFO_ACCTSTR:
      case SQL_ATTR_INFO_APPLNAME:
      case SQL_ATTR_INFO_USERID:
      case SQL_ATTR_INFO_WRKSTNNAME:
#endif
        retsv = sv_2mortal( newSVpv( (char*)ValuePtr, (int)StringLength ) );
        break;

      /* Integers */
      case SQL_ATTR_ACCESS_MODE:
#ifndef AS400
      case SQL_ATTR_CLOSE_BEHAVIOR:
      case SQL_ATTR_CONNECT_NODE:
      case SQL_ATTR_DB2ESTIMATE:
      case SQL_ATTR_DB2EXPLAIN:
      case SQL_ATTR_QUIET_MODE:
      case SQL_ATTR_TXN_ISOLATION:
#endif
        retsv = sv_2mortal( newSViv( (IV)( *(SQLINTEGER*)ValuePtr ) ) );
        break;

      default:
        break;
    }
  }

  if( ValuePtr != (SQLPOINTER)buffer )
    Safefree( ValuePtr );  /* Free dynamically allocated buffer */

  return retsv;
}                                                               


static int dbd_describe( SV *sth,                                
                         imp_sth_t *imp_sth )
{
    D_imp_dbh_from_sth;
    SQLCHAR  *cbuf_ptr;
    SQLINTEGER t_cbufl=0;
    short num_fields;
    SQLINTEGER i;
    SQLRETURN ret;                                               
    imp_fbh_t *fbh;                                              
    SQLINTEGER bufferSizeRequired;                               

    if (imp_sth->done_desc)
        return 1;    /* success, already done it */

    ret = SQLNumResultCols(imp_sth->phstmt,&num_fields);
    ret = check_error( sth, ret, "SQLNumResultCols failed" );
    EOI(ret);

    /* If execute hasn't been called yet and num_fields is zero it */
    /* might mean that this is a CALL statement in which case we   */
    /* must wait until after the execute to describe.  Just return */
    /* without setting done_desc flag.                             */
    if( 0 == num_fields && !DBIc_ACTIVE( imp_sth ) )             
      return 1;                                                  

    imp_sth->done_desc = 1;

    /* Unbind previously bound columns */
    if( DBIc_NUM_FIELDS( imp_sth ) > 0 )                        
    {
      ret = SQLFreeStmt( imp_sth->phstmt, SQL_UNBIND );
      ret = check_error( sth, ret, "Error unbinding previous columns" );
      EOI(ret);
    }                                                           

    if( DBIc_NUM_FIELDS( imp_sth ) != num_fields )
    {
      /* DBI normally doesn't allow NUM_OF_FIELDS to be changed but I can */
      /* fool it by setting NUM_FIELDS to 0 first                         */
      D_imp_drh_from_dbh;
      SV *value = newSViv( num_fields );

      if( NULL == imp_drh->svNUM_OF_FIELDS )
      {
        imp_drh->svNUM_OF_FIELDS = newSVpv( "NUM_OF_FIELDS", 13 );
      }

      DBIc_NUM_FIELDS(imp_sth) = num_fields;                     

      SvREFCNT_dec( value );
    }

    if( 0 == num_fields )                                        
      return 1; /* Let's get out of here, nothing to do */       

    /* allocate field buffers if necessary */
    if( num_fields > imp_sth->numFieldsAllocated )              
    {
      if( imp_sth->numFieldsAllocated > 0 )
      {
        /* already have some fields allocated */
        Renew( imp_sth->fbh, num_fields, imp_fbh_t );
        /* zero out new fields */
        Zero( imp_sth->fbh + imp_sth->numFieldsAllocated,
              num_fields - imp_sth->numFieldsAllocated,
              imp_fbh_t );
        Safefree( imp_sth->fbh_cbuf );
      }
      else
      {
        Newz( 42, imp_sth->fbh, num_fields, imp_fbh_t );
      }

      /* allocate a buffer to hold all the column names    */
      Newz(42, imp_sth->fbh_cbuf,
          (num_fields * (MAX_COL_NAME_LEN+1)), SQLCHAR );

      imp_sth->numFieldsAllocated = num_fields;
    }                                                           
    cbuf_ptr = imp_sth->fbh_cbuf;

    /* Get number of fields and space needed for field names    */
    for(i=0; i < num_fields; ++i )                              
    {
      fbh = &imp_sth->fbh[i];
      fbh->cbufl = MAX_COL_NAME_LEN+1;
      bufferSizeRequired = 0;                                    

      ret = SQLDescribeCol( imp_sth->phstmt,
                            (SQLUSMALLINT) (i+1),
                            (SQLCHAR*) cbuf_ptr,
                            (SQLSMALLINT) MAX_COL_NAME_LEN,
                            (SQLSMALLINT*)&fbh->cbufl,
                            (SQLSMALLINT*)&fbh->dbtype,
                            (SQLUINTEGER*)&fbh->prec,
                            (SQLSMALLINT*)&fbh->scale,
                            (SQLSMALLINT*)&fbh->nullok );
      ret = check_error( sth, ret, "DescribeCol failed" );
      EOI(ret);
      fbh->imp_sth = imp_sth;
      fbh->cbuf    = cbuf_ptr;
      fbh->cbuf[fbh->cbufl] = '\0';     /* ensure null terminated    */
      cbuf_ptr += fbh->cbufl + 1;       /* increment name pointer    */

      /* Now define the storage for this field data.            */

#ifdef AS400
      if( SQL_SMALLINT == fbh->dbtype ||
	  SQL_INTEGER == fbh->dbtype)
        {
	  fbh->ftype = SQL_C_LONG;
	  fbh->rlen = bufferSizeRequired = sizeof(SQLINTEGER);
        }
      else if( SQL_DECIMAL == fbh->dbtype ||
	       SQL_NUMERIC == fbh->dbtype ||
	       SQL_DOUBLE == fbh->dbtype||
	       SQL_FLOAT == fbh->dbtype||
	       SQL_REAL == fbh->dbtype)
	{
	  fbh->ftype = SQL_C_DOUBLE;
	  fbh->rlen = bufferSizeRequired = sizeof(SQLDOUBLE);
	}
      else if(SQL_BLOB == fbh->dbtype ||
	      SQL_CLOB == fbh->dbtype ||
	      SQL_DBCLOB == fbh->dbtype)
	{
	  fbh->ftype = fbh->dbtype;
	      fbh->rlen = bufferSizeRequired = fbh->dsize = fbh->prec;
	}
      else
#endif

      if( SQL_BINARY == fbh->dbtype ||
          SQL_VARBINARY == fbh->dbtype ||
          SQL_LONGVARBINARY == fbh->dbtype ||
                     SQL_BLOB == fbh->dbtype )
      {
          fbh->ftype = SQL_C_BINARY;
          fbh->rlen = bufferSizeRequired = fbh->dsize = fbh->prec;
      }
      else
      {
          fbh->ftype = SQL_C_CHAR;
#ifdef AS400
          ret = SQLColAttributes( imp_sth->phstmt,
                                 i+1,
                                 SQL_DESC_DISPLAY_SIZE,
                                 NULL,
                                 0,
                                 NULL,
                                 &fbh->dsize );
#else
          ret = SQLColAttribute( imp_sth->phstmt,
                                 (SQLSMALLINT) (i+1),
                                 SQL_DESC_DISPLAY_SIZE,
                                 NULL,
                                 0,
                                 NULL,
                                 &fbh->dsize );
#endif
          ret = check_error( sth, ret, "ColAttribute failed" );
          EOI(ret);

          fbh->rlen = bufferSizeRequired = fbh->dsize+1;/* +1: STRING null terminator */
      }

      /* Limit buffer size based on LongReadLen for long column types */
      if( SQLTypeIsLong( fbh->dbtype ) )
      {
        unsigned int longReadLen = DBIc_LongReadLen( imp_sth );

        if( fbh->rlen > (int) longReadLen )
        {
          if( SQL_LONGVARBINARY == fbh->dbtype ||
              SQL_BLOB == fbh->dbtype ||
              0 == longReadLen )
            fbh->rlen = bufferSizeRequired = longReadLen;        
          else
            fbh->rlen = bufferSizeRequired = longReadLen+1; /* +1 for null terminator */
        }
      }

      /* Allocate output buffer */
      if( bufferSizeRequired > fbh->bufferSize )                 
      {
        Safefree( fbh->buffer );                                 
        fbh->bufferSize = bufferSizeRequired;                    
        Newc( 1, fbh->buffer, fbh->bufferSize, SQLCHAR, void* );
      }

      /* BIND */
      ret = SQLBindCol( imp_sth->phstmt,
                        (SQLUSMALLINT) (i+1),
                        fbh->ftype,
                        fbh->buffer,
                        fbh->bufferSize,
                        &fbh->rlen );
      if (ret == SQL_SUCCESS_WITH_INFO ) {
          warn("BindCol error on %s: %d", fbh->cbuf);
      } else {
          ret = check_error( sth, ret, "BindCol failed" );
          EOI(ret);
      }

      if (DBIS->debug >= 2)
          fbh_dump(fbh, i);
    }                                                           
    return 1;
}


static void dbd_preparse( imp_sth_t *imp_sth,                    
                          char *statement )
{
    bool in_literal = FALSE;
    SQLCHAR  *src, *start, *dest;
    phs_t phs_tpl;
    SV *phs_sv;
    int idx=0, style=0, laststyle=0;

    /* allocate room for copy of statement with spare capacity    */
    /* for editing ':1' into ':p1' so we can use obndrv.    */
    imp_sth->statement = (SQLCHAR *)safemalloc(strlen(statement) +
                            (DBIc_NUM_PARAMS(imp_sth)*4));

    /* initialise phs ready to be cloned per placeholder    */
    memset(&phs_tpl, '\0',sizeof(phs_tpl));
    phs_tpl.sv = NULL;                            /*@d182289rmi*/

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
        PerlIO_printf( DBILOGFP,                                 
                       "scanned %d distinct placeholders\n",
                       (SQLINTEGER)DBIc_NUM_PARAMS(imp_sth) );
    }
}


int dbd_st_table_info( SV *sth,
                       imp_sth_t *imp_sth,                       
                       SV *attribs )                             
{
  D_imp_dbh_from_sth;                                            
  SQLRETURN ret;                                                 
  const SQLCHAR *pszSchema = "%";                               
  const SQLCHAR *pszTable = "%";
        SQLCHAR *pszTableType = NULL;
  SQLINTEGER cbSchemaLength = 1;
  SQLINTEGER cbTableLength = 1;
  SQLINTEGER cbTableTypeLength = 0;                             

  imp_sth->done_desc = 0;

  ret = SQLAllocHandle( SQL_HANDLE_STMT,
                        imp_dbh->hdbc,
                        &imp_sth->phstmt );
  ret = check_error( sth,
                     ret,
                     "Statement allocation error" );
  if( SQL_SUCCESS != ret )
     return 0;

  DBIc_IMPSET_on( imp_sth );  /* Resources allocated */

  if( attribs )                                                 
  {
    SV **svp;
    STRLEN len;

    if( ( svp = hv_fetch( (HV*)SvRV(attribs),
                          "TABLE_SCHEM",
                          11, 0 ) ) != NULL )
    {
      pszSchema = (SQLCHAR*)SvPV( *svp, len );
      cbSchemaLength = len;
    }

    if( ( svp = hv_fetch( (HV*)SvRV(attribs),
                          "TABLE_NAME",
                          10, 0 ) ) != NULL )
    {
      pszTable = (SQLCHAR*)SvPV( *svp, len );
      cbTableLength = len;
    }

    if( ( svp = hv_fetch( (HV*)SvRV(attribs),
                          "TABLE_TYPE",
                          10, 0 ) ) != NULL )
    {
      pszTableType = (SQLCHAR*)SvPV( *svp, len );
      cbTableTypeLength = len;
      /* CLI requires uppercase tokens */
      while( len-- > 0 )
      {
        pszTableType[len] = toupper( pszTableType[len] );
      }
    }
  }                                                             

  ret = SQLTables( imp_sth->phstmt,
                   NULL,
                   0,
                   (SQLCHAR*)pszSchema,                         
                   (SQLSMALLINT)cbSchemaLength,
                   (SQLCHAR*)pszTable,
                   (SQLSMALLINT)cbTableLength,
                   (SQLCHAR*)pszTableType,
                   (SQLSMALLINT)cbTableTypeLength );            
  ret = check_error( sth, ret, "SQLTables failed" );
  if( SQL_SUCCESS != ret )
     return 0;

  DBIc_NUM_PARAMS(imp_sth) = 0;
  DBIc_ACTIVE_on(imp_sth);

                                                                
  /* initialize sth pointers */
  imp_sth->RowCount = -1;
  imp_sth->bHasInput = 0;
  imp_sth->bHasOutput = 0;
  imp_sth->bMoreResults = 0;                                     
                                                                

  if( !dbd_describe( sth, imp_sth ) )
     return 0;

  return 1;
}                                                               

                                                                
int dbd_st_primary_key_info( SV        *sth,
                             imp_sth_t *imp_sth,
                             char      *pszCatalog,
                             char      *pszSchema,
                             char      *pszTable )
{
   D_imp_dbh_from_sth;
   SQLRETURN ret;
   SQLSMALLINT cbCatalogLength = 0;
   SQLSMALLINT cbSchemaLength  = 0;
   SQLSMALLINT cbTableLength   = 0;

   imp_sth->done_desc = 0;

   ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &imp_sth->phstmt );

   ret = check_error( sth, ret, "Statement allocation error" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_IMPSET_on( imp_sth );

   if( pszCatalog != NULL )
   {
      cbCatalogLength = strlen( pszCatalog );
   }

   if( pszSchema != NULL )
   {
      cbSchemaLength = strlen( pszSchema );
   }

   if( pszTable != NULL )
   {
      cbTableLength = strlen( pszTable );
   }

   ret = SQLPrimaryKeys( imp_sth->phstmt,
                         pszCatalog,
                         cbCatalogLength,
                         pszSchema,
                         cbSchemaLength,
                         pszTable,
                         cbTableLength );

   ret = check_error( sth, ret, "SQLPrimaryKeys failed" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_NUM_PARAMS( imp_sth ) = 0;
   DBIc_ACTIVE_on( imp_sth );

   imp_sth->RowCount = -1;
   imp_sth->bHasInput = 0;
   imp_sth->bHasOutput = 0;
   imp_sth->bMoreResults = 0;

   if( !dbd_describe( sth, imp_sth ) )
   {
      return 0;
   }

   return 1;
}
                                                                
                                                                
int dbd_st_foreign_key_info( SV        *sth,
                             imp_sth_t *imp_sth,
                             char      *pkCatalog,
                             char      *pkSchema,
                             char      *pkTable,
                             char      *fkCatalog,
                             char      *fkSchema,
                             char      *fkTable )
{
   D_imp_dbh_from_sth;
   SQLRETURN ret;
   SQLSMALLINT cbpkCatalogLength = 0;
   SQLSMALLINT cbpkSchemaLength  = 0;
   SQLSMALLINT cbpkTableLength   = 0;
   SQLSMALLINT cbfkCatalogLength = 0;
   SQLSMALLINT cbfkSchemaLength  = 0;
   SQLSMALLINT cbfkTableLength   = 0;

   imp_sth->done_desc = 0;

   ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &imp_sth->phstmt );

   ret = check_error( sth, ret, "Statement allocation error" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_IMPSET_on( imp_sth );

   if( pkCatalog != NULL )
   {
      cbpkCatalogLength = strlen( pkCatalog );
   }

   if( pkSchema != NULL )
   {
      cbpkSchemaLength = strlen( pkSchema );
   }

   if( pkTable != NULL )
   {
      cbpkTableLength = strlen( pkTable );
   }

   if( fkCatalog != NULL )
   {
      cbfkCatalogLength = strlen( fkCatalog );
   }

   if( fkSchema != NULL )
   {
      cbfkSchemaLength = strlen( fkSchema );
   }

   if( fkTable != NULL )
   {
      cbfkTableLength = strlen( fkTable );
   }

   ret = SQLForeignKeys( imp_sth->phstmt,
                         pkCatalog,
                         cbpkCatalogLength,
                         pkSchema,
                         cbpkSchemaLength,
                         pkTable,
                         cbpkTableLength,
                         fkCatalog,
                         cbfkCatalogLength,
                         fkSchema,
                         cbfkSchemaLength,
                         fkTable,
                         cbfkTableLength );

   ret = check_error( sth, ret, "SQLForeignKeys failed" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_NUM_PARAMS( imp_sth ) = 0;
   DBIc_ACTIVE_on( imp_sth );

   imp_sth->RowCount = -1;
   imp_sth->bHasInput = 0;
   imp_sth->bHasOutput = 0;
   imp_sth->bMoreResults = 0;

   if( !dbd_describe( sth, imp_sth ) )
   {
      return 0;
   }

   return 1;
}

                                                                
                                                                
int dbd_st_column_info( SV        *sth,
                        imp_sth_t *imp_sth,
                        char      *pszCatalog,
                        char      *pszSchema,
                        char      *pszTable,
                        char      *pszColumn )
{
   D_imp_dbh_from_sth;
   SQLRETURN ret;
   SQLSMALLINT cbCatalogLength = 0;
   SQLSMALLINT cbSchemaLength  = 0;
   SQLSMALLINT cbTableLength   = 0;
   SQLSMALLINT cbColumnLength  = 0;

   imp_sth->done_desc = 0;

   ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &imp_sth->phstmt );

   ret = check_error( sth, ret, "Statement allocation error" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_IMPSET_on( imp_sth );

   if( pszCatalog != NULL )
   {
      cbCatalogLength = strlen( pszCatalog );
   }

   if( pszSchema == NULL )
   {
      pszSchema = "%";
   }
   cbSchemaLength = strlen( pszSchema );

   if( pszTable == NULL )
   {
      pszTable = "%";
   }
   cbTableLength = strlen( pszTable );

   if( pszColumn == NULL )
   {
      pszColumn = "%";
   }
   cbColumnLength = strlen( pszColumn );


   ret = SQLColumns( imp_sth->phstmt,
                     pszCatalog,
                     cbCatalogLength,
                     pszSchema,
                     cbSchemaLength,
                     pszTable,
                     cbTableLength,
                     pszColumn,
                     cbColumnLength );

   ret = check_error( sth, ret, "SQLColumns failed" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_NUM_PARAMS( imp_sth ) = 0;
   DBIc_ACTIVE_on( imp_sth );

   imp_sth->RowCount = -1;
   imp_sth->bHasInput = 0;
   imp_sth->bHasOutput = 0;
   imp_sth->bMoreResults = 0;

   if( !dbd_describe( sth, imp_sth ) )
   {
      return 0;
   }

   return 1;
}
                                                                
                                                                
int dbd_st_type_info_all( SV        *sth,
                          imp_sth_t *imp_sth )
{
   D_imp_dbh_from_sth;
   SQLRETURN ret;

   imp_sth->done_desc = 0;

   ret = SQLAllocHandle( SQL_HANDLE_STMT, imp_dbh->hdbc, &imp_sth->phstmt );

   ret = check_error( sth, ret, "Statement allocation error" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_IMPSET_on( imp_sth );

   ret = SQLGetTypeInfo( imp_sth->phstmt, SQL_ALL_TYPES );

   ret = check_error( sth, ret, "SQLGetTypeInfo failed" );

   if( ret != SQL_SUCCESS )
   {
      return 0;
   }

   DBIc_NUM_PARAMS( imp_sth ) = 0;
   DBIc_ACTIVE_on( imp_sth );

   imp_sth->RowCount = -1;
   imp_sth->bHasInput = 0;
   imp_sth->bHasOutput = 0;
   imp_sth->bMoreResults = 0;

   if( !dbd_describe( sth, imp_sth ) )
   {
      return 0;
   }

   return 1;
}
                                                                
                                                                
SV *dbd_db_get_info( SV        *dbh,
                     imp_dbh_t *imp_dbh,
                     short      infoType )
{
   SV *retsv = &PL_sv_undef;
   SQLRETURN ret;
   char buffer[256];     /* should be big enough for all infotypes */
   SQLPOINTER valuePtr = (SQLPOINTER) buffer;
   SQLSMALLINT bufferLength = sizeof( buffer );
   SQLSMALLINT stringLength;

   memset( &buffer, '\0', sizeof( buffer ) );

   ret = SQLGetInfo( imp_dbh->hdbc,
                     infoType,
                     valuePtr,
                     bufferLength,
                     &stringLength );

   if( ret == SQL_SUCCESS_WITH_INFO &&  bufferLength < (stringLength + 1) )
   {
      if( DBIS->debug >= 2)
      {
         PerlIO_printf( DBILOGFP,
            "GetInfo(%d) local buffer isn't big enough. stringlenght=%d\n",
            infoType, stringLength );
      }

      /* Local buffer isn't big enough, dynamically allocate new one */
      bufferLength = stringLength + 1;
      Newc( 1, valuePtr, bufferLength, char, SQLPOINTER );
      Zero( valuePtr, bufferLength, char );

      ret = SQLGetInfo( imp_dbh->hdbc,
                        infoType,
                        valuePtr,
                        bufferLength,
                        &stringLength );
   }

   ret = check_error( dbh, ret, "Error calling SQLGetInfo" );

   /* Create our scalar value to return to app */
   if( ret == SQL_SUCCESS )
   {
      switch( infoType )
      {
         /* Create a scalar to hold the string infoTypes */
         case SQL_DBMS_NAME:
         case SQL_DBMS_VER:
         case SQL_CATALOG_NAME:
         case SQL_CATALOG_NAME_SEPARATOR:
         case SQL_ACCESSIBLE_PROCEDURES:
         case SQL_ACCESSIBLE_TABLES:
         case SQL_CATALOG_TERM:
         case SQL_COLLATION_SEQ:
         case SQL_COLUMN_ALIAS:
         case SQL_DATA_SOURCE_NAME:
         case SQL_DATA_SOURCE_READ_ONLY:
         case SQL_DATABASE_NAME:
         case SQL_DESCRIBE_PARAMETER:
         case SQL_DRIVER_NAME:
         case SQL_DRIVER_ODBC_VER:
         case SQL_DRIVER_VER:
         case SQL_EXPRESSIONS_IN_ORDERBY:
         case SQL_IDENTIFIER_QUOTE_CHAR:
         case SQL_INTEGRITY:
         case SQL_KEYWORDS:
         case SQL_LIKE_ESCAPE_CLAUSE:
         case SQL_MULT_RESULT_SETS:
         case SQL_MULTIPLE_ACTIVE_TXN:
         case SQL_NEED_LONG_DATA_LEN:
         case SQL_ODBC_VER:
         case SQL_ORDER_BY_COLUMNS_IN_SELECT:
         case SQL_OUTER_JOINS:
         case SQL_PROCEDURE_TERM:
         case SQL_PROCEDURES:
         case SQL_ROW_UPDATES:
         case SQL_SCHEMA_TERM:
         case SQL_SEARCH_PATTERN_ESCAPE:
         case SQL_SERVER_NAME:
         case SQL_SPECIAL_CHARACTERS:
         case SQL_TABLE_TERM:
         case SQL_USER_NAME:
         case SQL_XOPEN_CLI_YEAR:

            retsv = sv_2mortal( newSVpvn( (char*) valuePtr, (int)stringLength ) );
            break;

         /* Create a scalar to hold the 16-bit integer */
         case SQL_CATALOG_LOCATION:
         case SQL_CONCAT_NULL_BEHAVIOR:
         case SQL_CORRELATION_NAME:
         case SQL_CURSOR_COMMIT_BEHAVIOR:
         case SQL_CURSOR_ROLLBACK_BEHAVIOR:
         case SQL_FILE_USAGE:
         case SQL_GROUP_BY:
         case SQL_IDENTIFIER_CASE:
         case SQL_MAX_CATALOG_NAME_LEN:
         case SQL_MAX_COLUMN_NAME_LEN:
         case SQL_MAX_COLUMNS_IN_INDEX:
         case SQL_MAX_COLUMNS_IN_ORDER_BY:
         case SQL_MAX_COLUMNS_IN_SELECT:
         case SQL_MAX_COLUMNS_IN_TABLE:
         case SQL_MAX_CONCURRENT_ACTIVITIES:
         case SQL_MAX_CURSOR_NAME_LEN:
         case SQL_MAX_DRIVER_CONNECTIONS:
         case SQL_MAX_IDENTIFIER_LEN:
         case SQL_MAX_TABLE_NAME_LEN:
         case SQL_MAX_TABLES_IN_SELECT:
         case SQL_MAX_USER_NAME_LEN:
         case SQL_NON_NULLABLE_COLUMNS:
         case SQL_NULL_COLLATION:
         case SQL_ODBC_API_CONFORMANCE:
         case SQL_ODBC_SAG_CLI_CONFORMANCE:
         case SQL_ODBC_SQL_CONFORMANCE:
         case SQL_QUOTED_IDENTIFIER_CASE:
         case SQL_TXN_CAPABLE:

            retsv = sv_2mortal( newSViv( (I16)( *(SQLSMALLINT*)valuePtr) ) );
            break;

         /* Create a scalar to hold a 32bit integer */
         case 2519:                     /* SQL_DATABASE_CODEPAGE:    */
         case 2520:                     /* SQL_APPLICATION_CODEPAGE: */
         case 2521:                     /* SQL_CONNECT_CODEPAGE:     */
         case SQL_ASYNC_MODE:
         case SQL_BATCH_ROW_COUNT:
         case SQL_CURSOR_SENSITIVITY:
         case SQL_DATETIME_LITERALS:
         case SQL_DDL_INDEX:
         case SQL_DRIVER_HDBC:
         case SQL_DRIVER_HDESC:
         case SQL_DRIVER_HENV:
         case SQL_DROP_ASSERTION:
         case SQL_DROP_CHARACTER_SET:
         case SQL_DROP_COLLATION:
         case SQL_DROP_DOMAIN:
         case SQL_DROP_SCHEMA:
         case SQL_DROP_TABLE:
         case SQL_DROP_TRANSLATION:
         case SQL_DROP_VIEW:
         case SQL_DTC_TRANSITION_COST:
         case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
         case SQL_MAX_BINARY_LITERAL_LEN:
         case SQL_MAX_CHAR_LITERAL_LEN:
         case SQL_MAX_COLUMNS_IN_GROUP_BY:
         case SQL_MAX_INDEX_SIZE:
         case SQL_ODBC_INTERFACE_CONFORMANCE:
         case SQL_PARAM_ARRAY_ROW_COUNTS:
         case SQL_PARAM_ARRAY_SELECTS:
         case SQL_SQL_CONFORMANCE:

            retsv = sv_2mortal( newSViv( (I32)( *(SQLINTEGER*)valuePtr) ) );
            break;


         /* Createa scalar to hold the 32-bit mask */
         /* not supported */
         case SQL_AGGREGATE_FUNCTIONS:
         case SQL_ALTER_DOMAIN:
         case SQL_ALTER_TABLE:
         case SQL_BATCH_SUPPORT:
         case SQL_BOOKMARK_PERSISTENCE:
         case SQL_CATALOG_USAGE:
         case SQL_CONVERT_BIGINT:
         case SQL_CONVERT_BINARY:
         case SQL_CONVERT_BIT:
         case SQL_CONVERT_CHAR:
         case SQL_CONVERT_DATE:
         case SQL_CONVERT_DECIMAL:
         case SQL_CONVERT_DOUBLE:
         case SQL_CONVERT_FLOAT:
         case SQL_CONVERT_INTEGER:
         case SQL_CONVERT_INTERVAL_YEAR_MONTH:
         case SQL_CONVERT_INTERVAL_DAY_TIME:
         case SQL_CONVERT_LONGVARBINARY:
         case SQL_CONVERT_LONGVARCHAR:
         case SQL_CONVERT_NUMERIC:
         case SQL_CONVERT_REAL:
         case SQL_CONVERT_SMALLINT:
         case SQL_CONVERT_TIME:
         case SQL_CONVERT_TIMESTAMP:
         case SQL_CONVERT_TINYINT:
         case SQL_CONVERT_VARBINARY:
         case SQL_CONVERT_VARCHAR:
         case SQL_CONVERT_WCHAR:
         case SQL_CONVERT_WLONGVARCHAR:
         case SQL_CONVERT_WVARCHAR:
         case SQL_CONVERT_FUNCTIONS:
         case SQL_CREATE_ASSERTION:
         case SQL_CREATE_CHARACTER_SET:
         case SQL_CREATE_COLLATION:
         case SQL_CREATE_DOMAIN:
         case SQL_CREATE_SCHEMA:
         case SQL_CREATE_TABLE:
         case SQL_CREATE_TRANSLATION:
         case SQL_CREATE_VIEW:
         case SQL_DEFAULT_TXN_ISOLATION:
         /*case SQL_DYNAMIC_CURSOR_ATTRIBUTES:*/
         case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
         case SQL_FETCH_DIRECTION:
         case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
         case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
         case SQL_GETDATA_EXTENSIONS:
         case SQL_INDEX_KEYWORDS:
         case SQL_INFO_SCHEMA_VIEWS:
         case SQL_INSERT_STATEMENT:
         case SQL_KEYSET_CURSOR_ATTRIBUTES1:
         case SQL_KEYSET_CURSOR_ATTRIBUTES2:
         case SQL_LOCK_TYPES:
         case SQL_NUMERIC_FUNCTIONS:
         case SQL_OJ_CAPABILITIES:
         case SQL_POS_OPERATIONS:
         case SQL_POSITIONED_STATEMENTS:
         case SQL_SCHEMA_USAGE:
         case SQL_SCROLL_CONCURRENCY:
         case SQL_SCROLL_OPTIONS:
         case SQL_SQL92_DATETIME_FUNCTIONS:
         case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
         case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
         case SQL_SQL92_GRANT:
         case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
         case SQL_SQL92_PREDICATES:
         case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
         case SQL_SQL92_REVOKE:
         case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
         case SQL_SQL92_STRING_FUNCTIONS:
         case SQL_SQL92_VALUE_EXPRESSIONS:
         case SQL_STANDARD_CLI_CONFORMANCE:
         case SQL_STATIC_CURSOR_ATTRIBUTES1:
         case SQL_STATIC_CURSOR_ATTRIBUTES2:
         case SQL_STATIC_SENSITIVITY:
         case SQL_STRING_FUNCTIONS:
         case SQL_SUBQUERIES:
         case SQL_SYSTEM_FUNCTIONS:
         case SQL_TIMEDATE_ADD_INTERVALS:
         case SQL_TIMEDATE_DIFF_INTERVALS:
         case SQL_TIMEDATE_FUNCTIONS:
         case SQL_TXN_ISOLATION_OPTION:
         case SQL_UNION:

            /* how do return a bitmask? */
            /* retsv = sv_2mortal( newSVpv( (char*) valuePtr, (int) 32 ) ); */
            /* retsv = sv_2mortal( newSVrv( (SV*) valuePtr, NULL ) ); */
            break;


         default:
            break;
      }
   }

   /* Free dynamically allocated buffer */
   if( valuePtr != (SQLPOINTER) buffer )
   {
      Safefree( valuePtr );
   }

   return retsv;
}
                                                                

int dbd_st_prepare( SV *sth,
                    imp_sth_t *imp_sth,
                    char *statement,
                    SV *attribs )
{
    D_imp_dbh_from_sth;
    SQLRETURN ret;                                               
    SQLSMALLINT params;                                          

    /* initialize sth fields */                                  
    imp_sth->numFieldsAllocated = 0;                             
    imp_sth->RowCount = -1;                                       
    imp_sth->bHasInput = 0;                                      
    imp_sth->bHasOutput = 0;                                     
    imp_sth->bMoreResults = 0;                                   
    imp_sth->done_desc = 0;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, imp_dbh->hdbc,          
                         &imp_sth->phstmt);                       
    ret = check_error( sth, ret, "Statement allocation error" );
    EOI(ret);

    DBIc_IMPSET_on( imp_sth );  /* Resources allocated */        

    ret = SQLPrepare(imp_sth->phstmt,(SQLCHAR *)statement,SQL_NTS);
    ret = check_error( sth, ret, "Statement preparation error" );
    EOI(ret);

    if (DBIS->debug >= 2)                         /*@d150150rmi*/
      PerlIO_printf( DBILOGFP,                                   
                     "    dbd_st_prepare'd sql f%d\n\t%s\n",
                     imp_sth->phstmt, statement );               

    ret = SQLNumParams(imp_sth->phstmt,&params);
    ret = check_error( sth, ret, "Unable to determine number of parameters" );
    EOI(ret);

    DBIc_NUM_PARAMS(imp_sth) = params;

    if (params > 0 ){
        /* scan statement for '?', ':1' and/or ':foo' style placeholders*/
        dbd_preparse(imp_sth, statement);
    }

    dbd_describe( sth, imp_sth );                                

    return 1;
}


int dbd_bind_ph( SV *sth,                                       
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
    SQLRETURN ret;                                               
    short ctype = 0,
          scale = -1; /* initialize to invalid value */          
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
        pNum = atoi( (char*)name + 2 );                          
    }

    if (DBIS->debug >= 2)
      PerlIO_printf( DBILOGFP,                                   
                     "bind %s <== '%s' (attribs: %s)\n",
                     name,
                     SvPV(value,na), attribs ? SvPV(attribs,na) : "<no attribs>" );

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
            ( ( svp = hv_fetch( (HV*)SvRV(attribs),              
                                "db2_param_type",                
                                14, 0 ) ) != NULL ||             
              ( svp = hv_fetch( (HV*)SvRV(attribs),
                                "ParamT", 6, 0 ) ) != NULL ) )
            phs->paramType = (unsigned short) SvIV(*svp);
        if( ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "db2_type",                        
                              8, 0 ) ) != NULL ||                
            ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "TYPE", 4, 0 ) ) != NULL ||        
            ( svp = hv_fetch( (HV*)SvRV(attribs),
                              "Stype", 5, 0 ) ) != NULL )
            sql_type = SvIV(*svp);
        if( ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "db2_c_type",                      
                              10, 0 ) ) != NULL ||               
            ( svp = hv_fetch( (HV*)SvRV(attribs),
                              "Ctype", 5, 0 ) ) != NULL )
            ctype = (short) SvIV(*svp);
        if( ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "PRECISION",                       
                              9, 0 ) ) != NULL ||                
            ( svp = hv_fetch( (HV*)SvRV(attribs),
                              "Prec", 4, 0 ) ) != NULL )
            prec = SvIV(*svp);
        if( ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "SCALE",                           
                              5, 0 ) ) != NULL ||                
            ( svp = hv_fetch( (HV*)SvRV(attribs),
                              "Scale", 5, 0 ) ) != NULL )
            scale = (short) SvIV(*svp);
        if( ( svp = hv_fetch( (HV*)SvRV(attribs),                
                              "db2_file",                        
                              8, 0 ) ) != NULL ||                
            ( svp = hv_fetch( (HV*)SvRV(attribs),
                              "File", 4, 0 ) ) != NULL )
            bFile = (short) SvIV(*svp);
    } /* else if NULL / UNDEF then default to values assigned at top */
    /* This approach allows maximum performance when    */
    /* rebinding parameters often (for multiple executes).    */

    /* If the SQL type or scale haven't been specified, try to      */
    /* describe the parameter.  If this fails (it's an unregistered */
    /* stored proc for instance) then defaults will be used         */
    /* (SQL_VARCHAR and 0)                                          */
    if( 0 == sql_type ||
        ( -1 == scale &&                                        
          ( SQL_DECIMAL == sql_type ||
            SQL_NUMERIC == sql_type ||
            SQL_TIMESTAMP == sql_type ||
            SQL_TYPE_TIMESTAMP == sql_type ) ) )                
    {
      if( !phs->bDescribed )
      {
        SQLSMALLINT nullable;

        phs->bDescribed = TRUE;
        ret = SQLDescribeParam( imp_sth->phstmt,
                               pNum,
                               &phs->descSQLType,
                               &phs->descColumnSize,
                               &phs->descDecimalDigits,
                               &nullable );
        phs->bDescribeOK = ( SQL_SUCCESS == ret ||
                             SQL_SUCCESS_WITH_INFO == ret );
      }

      if( phs->bDescribeOK )
      {
        if( 0 == sql_type )
          sql_type = phs->descSQLType;

        if( -1 == scale )                                        
          scale = phs->descDecimalDigits;
      }
    }

    if( 0 == sql_type )
    {
      /* Still don't have an SQL type?  Set to default */
      sql_type = SQL_VARCHAR;
    }
    else if( -1 == scale )                                      
    {
      /* Still don't have a scale?  Set to default */
      scale = 0;
    }                                                           

    if( 0 == ctype )
    {
      /* Don't have a ctype yet?  Set to binary or char */
      if( SQLTypeIsBinary( (SQLSMALLINT) sql_type ) )
        ctype = SQL_C_BINARY;
#ifdef AS400
      else
	ctype = sql_type;
#else
      else
        ctype = SQL_C_CHAR;
#endif
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
        if( ((int)(value_len+1)) > phs->bufferSize )             
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
      PerlIO_printf( DBILOGFP,                                   
                     "  bind %s: "
                     "db2_param_type=%d, "                       
                     "db2_c_type=%d, "                           
                     "db2_type=%d, "                             
                     "PRECISION=%d, "                            
                     "SCALE=%d, "                                
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
                                                           : "Describe failed" ) );


    if( bFile &&
        SQL_PARAM_INPUT == phs->paramType &&
        ( SQL_BLOB == sql_type ||
          SQL_CLOB == sql_type ||
          SQL_DBCLOB == sql_type ) )
    {
      ret = SQLBindFileToParam( imp_sth->phstmt,
                                (SQLUSMALLINT)SvIV( param ),
                                (SQLSMALLINT) sql_type,
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
#ifdef AS400
      SQLPOINTER datap;
      switch (ctype){
      case SQL_C_SHORT:
      case SQL_C_LONG:
	datap = &SvIVX(value);
	break;
      case SQL_C_FLOAT:
      case SQL_C_DOUBLE:
	datap = &SvNVX(value);
	break;
      default:
	datap = phs->buffer;
	break;
      }
      ret = SQLBindParameter( imp_sth->phstmt,
                              (SQLUSMALLINT)SvIV( param ),
                              phs->paramType,
                              ctype,
                              sql_type,
                              phs->bDescribeOK ? phs->descColumnSize : prec,
                              scale,
                              datap,
                              maxlen,
                              &phs->indp );
#else
      ret = SQLBindParameter( imp_sth->phstmt,
                              (SQLUSMALLINT)SvIV( param ),
                              phs->paramType,
                              ctype,
                              (SQLSMALLINT) sql_type,
                              phs->bDescribeOK ? phs->descColumnSize : prec,
                              scale,
                              phs->buffer,
                              maxlen,
                              &phs->indp );
#endif
    }
    ret = check_error( sth, ret, "Bind failed" );
    EOI(ret);

    return 1;
}                                                               


int dbd_conn_opt( SV *sth,                                       
                  IV opt,
                  IV value )
{
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    SQLRETURN ret;                                               

    ret = SQLSetConnectAttr( imp_dbh->hdbc,
                             (SQLINTEGER) opt,
                             (SQLPOINTER) value,
                             (SQLINTEGER) 0 );

    ret = check_error( sth, ret, "SQLSetConnectOption failed" );
    EOI(ret);

    return 1;
}


int dbd_st_execute( SV *sth,     /* error : <=(-2), ok row count : >=0, unknown count : (-1)     */
                    imp_sth_t *imp_sth )
{
    SQLRETURN ret;                                               
    HV *hv;
    SV *sv;
    char *key;
    I32 retlen;
    phs_t *phs;
    STRLEN value_len;

    /* Reset input size and reallocate buffer if necessary for in/out
       parameters */
    if( imp_sth->bind_names && imp_sth->bHasInput )             
    {
      hv = imp_sth->bind_names;

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
              if( (int)value_len > (phs->bufferSize-1) )
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

    ret = SQLExecute(imp_sth->phstmt);
    ret = check_error( sth, ret, "SQLExecute failed" );
    if (ret < 0)                                                 
        return(-2);

    /* describe and allocate storage for results        */
    if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth)) {
        /* dbd_describe has already called check_error()        */
        return -2;                                                
    }

    if( imp_sth->bind_names && imp_sth->bHasOutput )            
    {
      hv = imp_sth->bind_names;

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
              sv_setsv( phs->sv, &sv_undef ); /* undefine variable */
#ifndef AS400
            else if( SQL_NO_TOTAL == phs->indp )
            {
              sv_setsv( phs->sv, &sv_undef ); /* undefine variable */
              warn( "Number of bytes available to return "
                    "cannot be determined for parameter '%s'", key );
            }
#endif
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
    ret = check_error( sth, ret, "SQLRowCount failed" );         

    DBIc_ACTIVE_on(imp_sth);
    return imp_sth->RowCount;                                     
}


AV *dbd_st_fetch( SV *sth,                                       
                  imp_sth_t *imp_sth )
{
    D_imp_dbh_from_sth;
    SQLINTEGER num_fields = DBIc_NUM_FIELDS( imp_sth );          
    SQLINTEGER ChopBlanks;
    SQLINTEGER i;
    SQLRETURN ret;                                               
    AV *av;
    imp_fbh_t *fbh;                                              
    SV *sv;                                                      
    int arraylen;                                                

    /* Check that execute() was executed sucessfuly. This also implies    */
    /* that dbd_describe() executed sucessfuly so the memory buffers    */
    /* are allocated and bound.                        */
    if ( !DBIc_ACTIVE(imp_sth) ) {
        check_error( sth, -3, "no statement executing" );
        return Nullav;
    }

    ret = SQLFetch(imp_sth->phstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
      ret = check_error( sth, ret, "Fetch failed" );            
      if( SQL_NO_DATA_FOUND == ret )
      {
        /* End of result set, need to check for additional result sets */
        /* to determine if it's safe to finish the statement.          */
        ret = SQLMoreResults( imp_sth->phstmt );
        ret = check_error( sth, ret, "SQLMoreResults failed" );
        if( SQL_SUCCESS == ret )
        {
          /* There is at least one more result set, set flag indicating so */
          imp_sth->bMoreResults = 1;
        }
        else
        {
          imp_sth->bMoreResults = 0;
          dbd_st_finish(sth, imp_sth);
        }
      }
      else                                                      
      {
        if (DBIS->debug >= 3)                      /*@d150150rmi*/
          PerlIO_printf( DBILOGFP,                               
                         "    dbd_st_fetch failed, rc=%d", ret ); 
        dbd_st_finish(sth, imp_sth);                             
      }
      return Nullav;
    }

    av = DBIS->get_fbav(imp_sth);
    /* Reset array size if necessary */
    arraylen = av_len( av ) + 1;                                
    if( arraylen != num_fields )
    {
      int bReadonly = SvREADONLY(av);
      int len = av_len( av ) + 1;

      if( bReadonly )
          SvREADONLY_off( av );         /* DBI sets this readonly  */

      while( len < num_fields )
      {
        av_store( av, len++, newSViv( 0 ) );
      }

      while( len > num_fields )
      {
        SvREFCNT_dec( av_pop( av ) );
        len--;
      }

      if( bReadonly )
          SvREADONLY_on( av );
    }                                                           

    if (DBIS->debug >= 3)                                        
    PerlIO_printf( DBILOGFP,                                     
                   "    dbd_st_fetch %d fields\n", num_fields );

    ChopBlanks = DBIc_has( imp_sth, DBIcf_ChopBlanks );
    for( i = 0; i < num_fields; ++i )
    {                                                           
      fbh = &imp_sth->fbh[i];
      sv = AvARRAY(av)[i]; /* Note: we reuse the supplied SV    */

#ifdef AS400
      if( fbh->rlen == SQL_NTS )
	fbh->rlen = strlen( fbh->buffer );
      if( fbh->ftype == SQL_C_LONG ){
	if( fbh->rlen > -1 && fbh->bufferSize > 0 ){
	  sv_setiv(sv, *((SQLINTEGER*)fbh->buffer));
	}else{
	  fbh->indp = fbh->rlen;
	  fbh->rlen = 0;
	  (void)SvOK_off(sv);
	}
      }else if( fbh->ftype == SQL_C_DOUBLE ){
	if( fbh->rlen > -1 && fbh->bufferSize > 0 ){
	  sv_setnv(sv, *((SQLDOUBLE*)fbh->buffer));
	}else{
	  fbh->indp = fbh->rlen;
	  fbh->rlen = 0;
	  (void)SvOK_off(sv);
	}
      }else{
#endif

      if( fbh->rlen > -1 &&      /* normal case - column is not null */
          fbh->bufferSize > 0 )
      {
        int nullAdj = SQL_C_CHAR == fbh->ftype ? 1 : 0;

        if( fbh->rlen > ( fbh->bufferSize - nullAdj ) ) /* data has been truncated */
        {
          int longTruncOk = DBIc_has( imp_sth, DBIcf_LongTruncOk );
          char msg[200];

          sv_setpvn( sv,
                     (char*)fbh->buffer,
                     fbh->bufferSize - nullAdj );                

          sprintf( msg,
                   "%s: Data in column %d has been truncated to %d bytes."
                   "  A maximum of %d bytes are available",
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
        fbh->indp = (short) fbh->rlen;
        fbh->rlen = 0;
        (void)SvOK_off(sv);
      }

#ifdef AS400
      }
#endif

      if( DBIS->debug >= 2 )
        PerlIO_printf( DBILOGFP,                                 
                       "\t%d: rc=%d '%s'\n", i, ret, SvPV(sv,na) );
    }                                                           
    return av;
}


int dbd_st_blob_read( SV *sth,                    /*@d157166rmi*/
                      imp_sth_t *imp_sth,
                      int field,
                      long offset,
                      long len,
                      SV *destrv,
                      long destoffset )
{
    D_imp_dbh_from_sth;
    SQLINTEGER retl;                                             
    SV *bufsv;
    SQLRETURN rtval;                                            
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
    SvGROW( bufsv, (STRLEN)(destoffset + len + cbNullSize) );

    rtval = SQLGetData( imp_sth->phstmt,
                        (SQLUSMALLINT) field,
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

    rtval = check_error( sth, rtval, "GetData failed to read LOB" );
    EOI(rtval);

    SvCUR_set(bufsv, destoffset+retl );
    *SvEND(bufsv) = '\0'; /* consistent with perl sv_setpvn etc    */

    return 1;
}


int dbd_st_rows( SV *sth,                                        
                 imp_sth_t *imp_sth )
{
    return imp_sth->RowCount;                                     
}


int dbd_st_finish( SV *sth,                                      
                   imp_sth_t *imp_sth )
{
    D_imp_dbh_from_sth;
    SQLRETURN ret;                                               

    /* Cancel further fetches from this cursor.  We don't        */ 
    /* close the cursor (SQLFreeHandle) 'til DESTROY (dbd_st_destroy).*/
    /* The application may call execute(...) again on the same   */
    /* statement handle.                                         */ 

    if (DBIc_ACTIVE(imp_sth) ) {
        ret = SQLFreeStmt(imp_sth->phstmt,SQL_CLOSE);
        ret = check_error( sth, ret, "SQLFreeStmt failed" );
        EOI(ret);
    }
    DBIc_ACTIVE_off(imp_sth);
    return 1;
}


void dbd_st_destroy( SV *sth,                                    
                     imp_sth_t *imp_sth )
{
    D_imp_dbh_from_sth;
    SQLINTEGER i;

    /* Free off contents of imp_sth    */

    for( i = 0; i < imp_sth->numFieldsAllocated; ++i)            
    {
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

    if( DBIc_ACTIVE( imp_dbh ) &&                               
        !DBIc_IADESTROY( imp_sth ) &&
        SQL_NULL_HSTMT != imp_sth->phstmt )
    {
      i = SQLFreeHandle( SQL_HANDLE_STMT, imp_sth->phstmt );
      check_error( sth, (SQLRETURN) i, "Statement destruction error" );      
      imp_sth->phstmt = SQL_NULL_HSTMT;
    }                                                           

    DBIc_IMPSET_off( imp_sth );  /* let DBI know we've done it */
}


static SQLINTEGER getStatementAttr( char *key,                  
                                    STRLEN keylen )
{
  /* For better performance, the keys are sorted by length */
  switch( keylen )
  {
#ifndef AS400
    case 10:
      if(      strEQ( key, "db2_noscan" ) )
        return SQL_ATTR_NOSCAN;
      return -1;

    case 12:
      if(      strEQ( key, "db2_max_rows" ) )
        return SQL_ATTR_MAX_ROWS;
      else if( strEQ( key, "db2_prefetch" ) )
        return SQL_ATTR_PREFETCH;
      return -1;

    case 14:
      if(      strEQ( key, "db2_earlyclose" ) )
        return SQL_ATTR_EARLYCLOSE;
      else if( strEQ( key, "db2_max_length" ) )
        return SQL_ATTR_MAX_LENGTH;
      else if( strEQ( key, "db2_row_number" ) )
        return SQL_ATTR_ROW_NUMBER;
      return -1;
#endif

    case 15:
      if(      strEQ( key, "db2_concurrency" ) )
        return SQL_ATTR_CONCURRENCY;
      else if( strEQ( key, "db2_cursor_hold" ) )
        return SQL_ATTR_CURSOR_HOLD;
      return -1;

#ifndef AS400
    case 17:
      if(      strEQ( key, "db2_query_timeout" ) )
        return SQL_ATTR_QUERY_TIMEOUT;
      else if( strEQ( key, "db2_retrieve_data" ) )
        return SQL_ATTR_RETRIEVE_DATA;
      else if( strEQ( key, "db2_txn_isolation" ) )
        return SQL_ATTR_TXN_ISOLATION;
      return -1;

    case 20:
      if(      strEQ( key, "db2_deferred_prepare" ) )
        return SQL_ATTR_DEFERRED_PREPARE;
      return -1;
#endif

    case 22:
      if(      strEQ( key, "db2_optimize_for_nrows" ) )
        return SQL_ATTR_OPTIMIZE_FOR_NROWS;
      return -1;

    case 28:
      if(      strEQ( key, "db2_query_optimization_level" ) )
        return SQL_ATTR_QUERY_OPTIMIZATION_LEVEL;
      return -1;

    default:
      return -1;
  }
}                                                               


int dbd_st_STORE_attrib( SV *sth,                                
                         imp_sth_t *imp_sth,
                         SV *keysv,
                         SV *valuesv )
{
  STRLEN kl;
  char *key = SvPV( keysv, kl );
  SQLINTEGER Attribute = getStatementAttr( key, kl );
  SQLRETURN ret;
  SQLPOINTER ValuePtr = 0;
  SQLINTEGER StringLength = 0;
  char msg[128]; /* buffer for error messages */
#ifdef AS400
  SQLPOINTER param;
#endif

  if( Attribute < 0 ) /* Don't know what this attribute is */
    return FALSE;

  switch( Attribute )
  {
    /* Booleans */
#ifndef AS400
    case SQL_ATTR_CURSOR_HOLD:
    case SQL_ATTR_DEFERRED_PREPARE:
    case SQL_ATTR_EARLYCLOSE:
    case SQL_ATTR_NOSCAN:
    case SQL_ATTR_PREFETCH:
    case SQL_ATTR_RETRIEVE_DATA:
      if( SvTRUE( valuesv ) )
        ValuePtr = (SQLPOINTER)1;
      break;
#else
    case SQL_ATTR_CURSOR_HOLD:
      param=SQL_TRUE;
      if( SvTRUE( valuesv ) )
        ValuePtr = (SQLPOINTER)&param;
      break;
#endif

    /* Integers */
    case SQL_ATTR_CONCURRENCY:
#ifndef AS400
    case SQL_ATTR_MAX_LENGTH:
    case SQL_ATTR_MAX_ROWS:
#endif
    case SQL_ATTR_OPTIMIZE_FOR_NROWS:
    case SQL_ATTR_QUERY_OPTIMIZATION_LEVEL:
#ifndef AS400
    case SQL_ATTR_QUERY_TIMEOUT:
    case SQL_ATTR_TXN_ISOLATION:
#endif
      if( SvIOK( valuesv ) )
      {
        ValuePtr = (SQLPOINTER)SvIV( valuesv );
      }
      else if( SvOK( valuesv ) )
      {
        /* Value is not an integer, return error */
        sprintf( msg,
                 "Invalid value for statement attribute %s, expecting integer",
                 key );
        do_error( sth, -1, 0, SQL_NULL_HANDLE, msg );
        return FALSE;
      }
      else /* Undefined, Set to default, most are 0 or NULL */
      {
      }
      break;

    default:
      return FALSE;
  }


  ret = SQLSetStmtAttr( imp_sth->phstmt,
                        Attribute,
                        ValuePtr,
                        StringLength );
  if( SQL_SUCCESS != ret )
  {
    sprintf( msg, "Error setting %s statement attribute", key );
    check_error( sth, ret, msg );
    return FALSE;
  }

  return TRUE;
}


SV *dbd_st_FETCH_attrib( SV *sth,                                
                         imp_sth_t *imp_sth,
                         SV *keysv )
{
  STRLEN kl;
  char *key = SvPV( keysv, kl );
  int i;
  SV *retsv = NULL;
  AV *av;
  int cacheit = 1;                                               
  SQLINTEGER Attribute;                                          

  if (!imp_sth->done_desc && !dbd_describe(sth, imp_sth))
  {
    /* dbd_describe has already called check_error()        */   
    /* We can't return Nullsv here because the xs code will */
    /* then just pass the attribute name to DBI for FETCH.  */
    croak("Describe failed during %s->FETCH(%s)",
           SvPV(sth,na), key);                                   
  }

  i = DBIc_NUM_FIELDS(imp_sth);

  if( kl == 7 && strEQ( key, "lengths" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( (SV*)av ) );                      
    while(--i >= 0)
        av_store(av, i, newSViv((IV)imp_sth->fbh[i].dsize));
  }
  else if( kl == 5 && strEQ( key, "types" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( (SV*)av ) );                      
    while(--i >= 0)
        av_store(av, i, newSViv(imp_sth->fbh[i].dbtype));
  }
  else if( kl == 13 && strEQ( key, "NUM_OF_PARAMS" ) )
  {
    HV *bn = imp_sth->bind_names;
    retsv = sv_2mortal( newSViv( (bn) ? HvKEYS(bn) : 0 ) );      
  }
  else if( kl == 4 && strEQ( key, "NAME" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( (SV*)av ) );                      
    while(--i >= 0)
        av_store(av, i, newSVpv((char *)imp_sth->fbh[i].cbuf,0));
  }
  else if( kl == 8 && strEQ( key, "NULLABLE" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( (SV*)av ) );                      
    while(--i >= 0)
        av_store(av, i,                                          
                 (imp_sth->fbh[i].nullok == 1) ? &sv_yes : &sv_no);
  }
  else if( kl == 10 && strEQ( key, "CursorName" ) )
  {
    char cursor_name[256];                                       
    SQLSMALLINT cursor_name_len;
    SQLRETURN ret =                                              
          SQLGetCursorName(imp_sth->phstmt, (SQLCHAR *)cursor_name,
                           sizeof(cursor_name), &cursor_name_len);
    ret = check_error( sth, ret, "SQLGetCursorName failed" );
    if (ret < 0)
        return Nullsv;
    else
        retsv = sv_2mortal( newSVpv(cursor_name, cursor_name_len) );
  }
  else if( kl == 4 && strEQ( key, "TYPE" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( sv_2mortal( (SV*)av ) ) );        
    while(--i >= 0)
        av_store(av, i, newSViv(imp_sth->fbh[i].dbtype));
  }
  else if( kl == 9 && strEQ( key, "PRECISION" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( sv_2mortal( (SV*)av ) ) );        
    while(--i >= 0)
        av_store(av, i, newSViv(imp_sth->fbh[i].prec));
  }
  else if( kl == 5 && strEQ( key, "SCALE" ) )
  {
    av = newAV();
    retsv = sv_2mortal( newRV( sv_2mortal( (SV*)av ) ) );        
    while(--i >= 0)
        av_store(av, i, newSViv(imp_sth->fbh[i].scale));         
  }
  else if( 16 == kl && strEQ( key, "db2_more_results" ) )       
  {
    if( !DBIc_ACTIVE(imp_sth) )
    {
      /* Statement has been finished, no more results available */
      retsv = &sv_no;
    }
    else if( imp_sth->bMoreResults )
    {
      /* Already know that there are more result sets */
      retsv = &sv_yes;
    }
    else
    {
      SQLRETURN ret = SQLMoreResults( imp_sth->phstmt );
      ret = check_error( sth, ret, "Error getting more results" );
      if( SQL_SUCCESS == ret )
      {
        retsv = &sv_yes;
      }
      else
      {
        /* No more results, finish statement */
        dbd_st_finish(sth, imp_sth);
        retsv = &sv_no;
      }
    }

    if( &sv_yes == retsv )
    {
      /* describe and allocate storage for results        */
      imp_sth->done_desc = FALSE;
                                                               
      /* Remove statement attribs in cache                */
      if (hv_exists((HV*) SvRV(sth), "NAME", 4) )
         hv_delete((HV *) SvRV(sth), "NAME", 4, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "NAME_uc", 7))
         hv_delete((HV *) SvRV(sth), "NAME_uc", 7, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "NAME_lc", 7) )
         hv_delete((HV *) SvRV(sth), "NAME_lc", 7, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "TYPE", 4) )
         hv_delete((HV *) SvRV(sth), "TYPE", 4, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "PRECISION", 9) )
         hv_delete((HV *) SvRV(sth), "PRECISION", 9, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "SCALE", 5) )
         hv_delete((HV *) SvRV(sth), "SCALE", 5, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "NULLABLE", 8) )
         hv_delete((HV *) SvRV(sth), "NULLABLE", 8, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "NUM_OF_FIELDS", 13) )
         hv_delete((HV *) SvRV(sth), "NUM_OF_FIELDS", 13, G_DISCARD);

      if (hv_exists((HV*) SvRV(sth), "CursorName", 10) )
         hv_delete((HV *) SvRV(sth), "CursorName", 10, G_DISCARD);
                                                               

      if( dbd_describe( sth, imp_sth ) )
      {
        /* dbd_describe has already called check_error() */
      }
      imp_sth->bMoreResults = 0;  /* reset to unknown */
    }

    cacheit = 0; /* Don't cache this attribute */
  }                                                             
  else if( ( Attribute = getStatementAttr( key, kl ) ) >= 0 )   
  {
    SQLRETURN ret;
    char buffer[128]; /* should be big enough for any attribute value */
    SQLPOINTER ValuePtr = (SQLPOINTER)buffer;
    SQLINTEGER BufferLength = sizeof( buffer );
    SQLINTEGER StringLength;

    ret = SQLGetStmtAttr( imp_sth->phstmt,
                          Attribute,
                          ValuePtr,
                          BufferLength,
                          &StringLength );
    if( SQL_SUCCESS_WITH_INFO == ret &&
        (StringLength + 1) > BufferLength )
    {
      /* local buffer isn't big enough, allocate one */
      BufferLength = StringLength + 1;
      Newc( 1, ValuePtr, BufferLength, char, SQLPOINTER );
      ret = SQLGetStmtAttr( imp_sth->phstmt,
                            Attribute,
                            ValuePtr,
                            BufferLength,
                            &StringLength );
    }

    ret = check_error( sth, ret, "Error retrieving statement attribute" );
    if( SQL_SUCCESS == ret )
    {
      switch( Attribute )
      {
        /* Booleans */
        case SQL_ATTR_CURSOR_HOLD:
#ifndef AS400
        case SQL_ATTR_DEFERRED_PREPARE:
        case SQL_ATTR_EARLYCLOSE:
        case SQL_ATTR_NOSCAN:
        case SQL_ATTR_PREFETCH:
        case SQL_ATTR_RETRIEVE_DATA:
#endif
          if( *(SQLINTEGER*)ValuePtr )
            retsv = &sv_yes;
          else
            retsv = &sv_no;
          break;

        /* Integers */
        case SQL_ATTR_CONCURRENCY:
#ifndef AS400
        case SQL_ATTR_MAX_LENGTH:
        case SQL_ATTR_MAX_ROWS:
#endif
        case SQL_ATTR_OPTIMIZE_FOR_NROWS:
        case SQL_ATTR_QUERY_OPTIMIZATION_LEVEL:
#ifndef AS400
        case SQL_ATTR_QUERY_TIMEOUT:
        case SQL_ATTR_ROW_NUMBER:
        case SQL_ATTR_TXN_ISOLATION:
#endif
          retsv = sv_2mortal( newSViv( (IV)( *(SQLINTEGER*)ValuePtr ) ) );
          break;

        default:
          break;
      }
    }

    if( ValuePtr != (SQLPOINTER)buffer )
      Safefree( ValuePtr );  /* Free dynamically allocated buffer */

    cacheit = 0; /* Don't cache CLI attributes */
  }                                                             
  else
  {
    return Nullsv;
  }

  if( cacheit )                                                  
  { /* cache for next time (via DBI quick_FETCH)    */
    SV **svp = hv_fetch((HV*)SvRV(sth), key, kl, 1);
    sv_free(*svp);
    *svp = retsv;
    (void)SvREFCNT_inc(retsv);    /* so sv_2mortal won't free it    */
  }

  return retsv;                                                  
}
