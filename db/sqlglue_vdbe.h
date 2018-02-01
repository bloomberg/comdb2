#ifndef SQLGLUE_VDBE_H
#define SQLGLUE_VDBE_H

#include "sql.h"

int sqlglue_get_data_int(BtCursor *, struct schema *, uint8_t *in, int fnum,
                         Mem *, uint8_t flip_orig, const char *tzname);

int is_datacopy(BtCursor *pCur, int *fnum);
int get_datacopy(BtCursor *pCur, int fnum, Mem *m);

inline int is_raw(BtCursor *pCur)
{
    if (!pCur)
        return 0;
    return (pCur->cursor_class == CURSORCLASS_TABLE ||
            pCur->cursor_class == CURSORCLASS_INDEX ||
            pCur->cursor_class == CURSORCLASS_REMOTE ||
            pCur->is_sampled_idx);
}



inline int is_remote(BtCursor *pCur)
{
    return pCur->cursor_class == CURSORCLASS_REMOTE;
}

inline int get_data(BtCursor *pCur, void *invoid, int fnum, Mem *m)
{
    if (unlikely(pCur->cursor_class == CURSORCLASS_REMOTE)) {
        /* convert the remote buffer to M array */
        abort(); /* this is suppsed to be a cooked access */
    } else {
        return sqlglue_get_data_int(pCur, pCur->sc, invoid, fnum, m, 0,
                            pCur->clnt->tzname);
    }
}


#endif
