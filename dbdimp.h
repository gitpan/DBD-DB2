/*
   $Id: dbdimp.h,v 0.10 1998/03/16 20:36:56 mhm Rel $

   Copyright (c) 1995,1996 International Business Machines Corp.
*/

/* these are (almost) random values ! */
#define MAX_COLS 99
#define MAX_COL_NAME_LEN 19
#define MAX_BIND_VARS	99


typedef struct imp_fbh_st imp_fbh_t;

struct imp_drh_st {
    dbih_drc_t com;				/* MUST be first element in structure	*/
	SQLHENV	henv;
	int 	connects;
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
    dbih_dbc_t com;				/* MUST be first element in structure	*/
	SQLHENV	henv;
	SQLHDBC	hdbc;
};


/* Define sth implementor data structure */
struct imp_sth_st {
    dbih_stc_t com;				/* MUST be first element in structure	*/
	SQLHENV	henv;
	SQLHDBC	hdbc;

	SQLHSTMT	phstmt;
    /* Input Details	*/
    SQLCHAR	*statement;  		/* sql (see sth_scan)		*/
    HV		*bind_names;

    /* Output Details		*/
    SQLINTEGER        done_desc;  		/* have we described this sth yet ?	*/
    imp_fbh_t *fbh;	    		/* array of imp_fbh_t structs	*/
    SQLCHAR      *fbh_cbuf;    	/* memory for all field names       */
    SQLINTEGER		long_buflen;      	/* length for long/longraw (if >0)	*/
    SQLCHAR 	long_trunc_ok;    	/* is truncating a long an error	*/
};
#define IMP_STH_EXECUTING	0x0001


struct imp_fbh_st { 	/* field buffer EXPERIMENTAL */
    imp_sth_t *imp_sth;	/* 'parent' statement */

    /* description of the field	*/
    SQLINTEGER  dbsize;
    short  dbtype;
    SQLCHAR	*cbuf;		/* ptr to name of select-list item */
    SQLINTEGER  cbufl;			/* length of select-list item name */
    SQLINTEGER  dsize;			/* max display size if field is a SQLCHAR */
    unsigned long prec;
    short  scale;
    short  nullok;

    /* Our storage space for the field data as it's fetched	*/
    short ftype;		/* external datatype we wish to get	*/
    short  indp;		/* null/trunc indicator variable	*/
    SQLCHAR	*buf;		/* data buffer (poSQLINTEGERs to sv data)	*/
    short  bufl;		/* length of data buffer		*/
    SQLINTEGER rlen;		/* length of returned data		*/
    short  rcode;		/* field level error status		*/

    SV	*sv;
};


typedef struct phs_st phs_t;    /* scalar placeholder   */

struct phs_st {	/* scalar placeholder EXPERIMENTAL	*/
    SV	*sv;		/* the scalar holding the value		*/
    short ftype;	/* external OCI field type		*/
    SQLINTEGER indp;		/* null indicator or length indicator */
};

SQLCHAR sql_state[6];
#ifdef Harvey
void	do_error _((SV *h,SQLINTEGER rc, SQLHENV henv, SQLHDBC hconn, 
					SQLHSTMT hstmt, char *what));
void	fbh_dump _((imp_fbh_t *fbh, int i));

void	dbd_init _((dbistate_t *dbistate));
void	dbd_preparse _((imp_sth_t *imp_sth, char *statement));
int	dbd_describe _((SV *h, imp_sth_t *imp_sth));
#endif


#define dbd_init        db2_init
#define dbd_db_login        db2_db_login
#define dbd_db_do       db2_db_do
#define dbd_db_commit       db2_db_commit
#define dbd_db_rollback     db2_db_rollback
#define dbd_db_disconnect   db2_db_disconnect
#define dbd_db_destroy      db2_db_destroy
#define dbd_db_STORE_attrib db2_db_STORE_attrib
#define dbd_db_FETCH_attrib db2_db_FETCH_attrib
#define dbd_st_prepare      db2_st_prepare
#define dbd_st_rows     db2_st_rows
#define dbd_st_execute      db2_st_execute
#define dbd_st_fetch        db2_st_fetch
#define dbd_st_finish       db2_st_finish
#define dbd_st_destroy      db2_st_destroy
#define dbd_st_blob_read    db2_st_blob_read
#define dbd_st_STORE_attrib db2_st_STORE_attrib
#define dbd_st_FETCH_attrib db2_st_FETCH_attrib
#define dbd_describe        db2_describe
#define dbd_bind_ph     db2_bind_ph

void	do_error _((SV *h,SQLINTEGER rc, SQLHENV henv, SQLHDBC hconn, 
					SQLHSTMT hstmt, char *what));
void	fbh_dump _((imp_fbh_t *fbh, int i));

void	dbd_init _((dbistate_t *dbistate));
void	dbd_preparse _((imp_sth_t *imp_sth, char *statement));
int		dbd_describe _((SV *h, imp_sth_t *imp_sth));
/* end */
