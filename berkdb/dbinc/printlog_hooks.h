#ifndef _COMDB2_DBPRINTLOG_
#define _COMDB2_DBPRINTLOG_

/* Called at the start of a print-log function. */
typedef int (printlog_start_t)(u_int32_t file, u_int32_t offset, u_int32_t 
	type,u_int32_t txnid, u_int32_t prev_file, u_int32_t prev_offset, void 
	*argp);

/* Called at the end of a print-log function. */
typedef int (printlog_end_t)(int file, int offset);

#endif
