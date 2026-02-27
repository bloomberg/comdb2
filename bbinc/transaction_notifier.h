#ifndef INCLUDED_TRANSACTION_NOTIFIER_H
#define INCLUDED_TRANSACTION_NOTIFIER_H

#include "list.h"

typedef struct transaction_notifier {
    void (*simple_notify)(void);
    LINKC_T(struct transaction_notifier) lnk;
} transaction_notifier;

typedef LISTC_T(struct transaction_notifier) transaction_notifier_list;

#endif
