/*
   $Id: dbdimp.h,v 0.5 1996/05/16 16:21:11 mhm Rel $

   Copyright (c) 1995,1996 International Business Machines Corp.
*/

/* these are (almost) random values ! */
#define MAX_COLS 99
#define MAX_COL_NAME_LEN 19
#define MAX_BIND_VARS	99


typedef struct imp_fbh_st imp_fbh_t;

struct imp_drh_st {
    dbih_drc_t com;				/* MUST be first element in structure	*/
};

/* Define dbh implementor data structure */
struct imp_dbh_st {
    dbih_dbc_t com;				/* MUST be first element in structure	*/
	SQLHDBC	hdbc;
};


/* Define sth implementor data structure */
struct imp_sth_st {
    dbih_stc_t com;				/* MUST be first element in structure	*/
	SQLHSTMT	phstmt;
    /* Input Details	*/
    char	*statement;  		/* sql (see sth_scan)		*/
    HV		*bind_names;

    /* Output Details		*/
    int        done_desc;  		/* have we described this sth yet ?	*/
    imp_fbh_t *fbh;	    		/* array of imp_fbh_t structs	*/
    char      *fbh_cbuf;    	/* memory for all field names       */
    int		long_buflen;      	/* length for long/longraw (if >0)	*/
    char 	long_trunc_ok;    	/* is truncating a long an error	*/
};
#define IMP_STH_EXECUTING	0x0001


struct imp_fbh_st { 	/* field buffer EXPERIMENTAL */
    imp_sth_t *imp_sth;	/* 'parent' statement */

    /* description of the field	*/
    int  dbsize;
    short  dbtype;
    char	*cbuf;		/* ptr to name of select-list item */
    int  cbufl;			/* length of select-list item name */
    int  dsize;			/* max display size if field is a char */
    unsigned long prec;
    short  scale;
    short  nullok;

    /* Our storage space for the field data as it's fetched	*/
    short ftype;		/* external datatype we wish to get	*/
    short  indp;		/* null/trunc indicator variable	*/
    char	*buf;		/* data buffer (points to sv data)	*/
    short  bufl;		/* length of data buffer		*/
    int rlen;		/* length of returned data		*/
    short  rcode;		/* field level error status		*/

    SV	*sv;
};


typedef struct phs_st phs_t;    /* scalar placeholder   */

struct phs_st {	/* scalar placeholder EXPERIMENTAL	*/
    SV	*sv;		/* the scalar holding the value		*/
    short ftype;	/* external OCI field type		*/
    int indp;		/* null indicator or length indicator */
};

char sql_state[6];

void	do_error _((SV *h,int rc, SQLHENV henv, SQLHDBC hconn, 
					SQLHSTMT hstmt, char *what));
void	fbh_dump _((imp_fbh_t *fbh, int i));

void	dbd_init _((dbistate_t *dbistate));
void	dbd_preparse _((imp_sth_t *imp_sth, char *statement));
int	dbd_describe _((SV *h, imp_sth_t *imp_sth));

/* end */
