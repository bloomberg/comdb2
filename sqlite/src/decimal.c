#include "sqlite3.h"
#include "logmsg.h"

int sqlite3DecimalToString( sql_decimal_t * dec, char *str, int len){
   const decQuad *quad = (const decQuad *)dec;
   char * ret = 0;

   if( len <= DECQUAD_Pmax+9 ){
      logmsg(LOGMSG_ERROR, "%s:%d %s conversion failure string too short %d<%d\n",
         __FILE__, __LINE__, __func__, len, DECQUAD_Pmax+9);
      return -1;
   }

   ret = decQuadToString(quad, str);
   if( !ret ){
      logmsg(LOGMSG_ERROR, "%s:%d %s conversion failure\n",
         __FILE__, __LINE__, __func__);
      return -1;
   }

   return 0;
}

