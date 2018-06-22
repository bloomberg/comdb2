/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <walkback.h>
#include <inttypes.h>

#ifdef DUMP_STACK
#include <errno.h>
#include <fcntl.h>
#endif

int gbl_walkback_enabled = 1;
int gbl_warnthresh = 50;

static unsigned long long count = 0, ivcount = 0;

/* Reset counter if we're enabling */
void walkback_set_warnthresh(int thresh)
{
    if (gbl_warnthresh == 0 && thresh > 0) ivcount = count;
    gbl_warnthresh = thresh;
}

int walkback_get_warnthresh(void)
{
    return gbl_warnthresh;
}

static void showfunc(void *stack[], int nframes, FILE *f)
{
    int i;
    if (nframes > 0) {
        fprintf(f, "showfunc.tsk [comdb2] ");
        for (i = 0; i < nframes; i++) {
            if (stack[i])
                fprintf(f, "%p ", stack[i]);
        }
        fprintf(f, "\n");
    }
}

static void trace_pc_getlist(void *stack[], int nframes, FILE *f)
{
    static int lastprint = 0;
    int now;

    count++;

/* Print a message if we've seen at least 'warnthresh' messages
 * in the last second */
#if 0
    if(gbl_warnthresh>0 && ((now=time(NULL))-lastprint)>=1)
    {
        if((count-ivcount) > gbl_warnthresh)
        {
            fprintf(f, "Exceeded walkback warn-threshold (%d/%d): ", 
                    (int)(count-ivcount), gbl_warnthresh);
            showfunc(stack, nframes, f);
        }
        ivcount=count;
        lastprint=now;
    }
#endif
}

#if defined(__sparc)

#include <stddef.h>
#include <sys/frame.h>
#include <sys/signal.h>
#include <thread.h>

/*
  According to frame.h the mininum frame size is offsetof(frame::fr_argx).
*/
static const int MIN_FRAME_SIZE = offsetof(struct frame, fr_argx);

#elif defined(_AIX)

/* This module must be compiled with -D_THREAD_SAFE */

#include <ucontext.h>
#include <errno.h>
#include <sys/debug.h>

struct frame {
    struct frame *fr_savfp;
    int fr_unknown;
    void *fr_savpc;
};

#elif defined(__linux__)

#define UNW_LOCAL_ONLY /* only async-safe code */
#ifdef USE_UNWIND
#include <libunwind.h>
#endif

#elif defined(__hpux)

#include <uwx.h>
#include <uwx_self.h>

#define ALLOCATOR_BUFSIZE 51200
#define MIN_ALIGNMENT 16

typedef struct {
    size_t cursor;
    char mem_buf[ALLOCATOR_BUFSIZE];
} walkback_alloc_t;

static void walkback_allocator_init(walkback_alloc_t *allocator);
static void *walkback_allocate(walkback_alloc_t *allocator, size_t size);
static void walkback_free(walkback_alloc_t *allocator, void *ptr);

#elif defined(__APPLE__)

#include <libunwind.h>

#else

#error Unsupported architecture

#endif

struct pclist_add_arg_t {
    void **pcArray;
    unsigned pcMaxCount;
    unsigned pcOutCount;
};

/*
  Walkback errors
*/

#define ENOCONTEXT (1)   /* getcontext() error             */
#define ECYCLE (2)       /* next frame below current frame */
#define EBELOWMIN (3)    /* next frame below the stack     */
#define EABOVEMAX (4)    /* next frame above the stack     */
#define ENOIMPL (5)      /* walkback not implemnted        */
#define ESTACKMAX (6)    /* max stack address not found    */
#define EUNWINDER (7)    /* Error in internal unwinder     */
#define EOPEN (8)        /* Error creating lock file       */
#define ECLOSE (9)       /* Error closing lock file        */
#define EUNLINK (10)     /* Error deleting lock file       */
#define EGBLCORRUPT (11) /* Global memory corrupted        */
#define EUNINIT (12)     /* Module uninitialized           */

#define EUNWINDERRR(i)                                                         \
    ((int)((uint32_t)(i) << 24 | (uint32_t)(__LINE__) << 8 | 7u))
#define EUNWINDLINE(i) ((int)(int16_t)((uint32_t)(i) >> 8))
#define EUNWINDCODE(i) ((int)(int8_t)((uint32_t)(i) >> 24))

#ifdef DUMP_STACK
static void dump_stack(void *min_stack_address, size_t nwords)
{
    FILE *fptr;
    char buf[24];
    char *p;
    int i;

    snprintf(buf, sizeof(buf), "stack.%d", getpid());
    fptr = fopen(buf, "a");
    if (fptr == NULL) {
        fprintf(stderr, "Could not open %s: %s\n", buf, strerror(errno));
        return;
    }
    p = (char *)min_stack_address + (nwords * sizeof(int));
    fprintf(fptr, "Dumping %d bytes from 0x%x to 0x%x\n", nwords * sizeof(int),
            min_stack_address, p - 1);
    p = (char *)min_stack_address + ((nwords - 4) * sizeof(int));
    for (i = nwords; i >= 0; i -= 4, p -= (4 * sizeof(int))) {
        int len = 4 * sizeof(int);
        if (i < 4) {
            len = i * sizeof(int);
        }
        snprintf(buf, sizeof(buf), "%8x", p);
        hex_ascii_dump(buf, p, len, fptr);
    }
    fclose(fptr);
}
#endif

/******************************************************************************
*
* Function: walkback_initialize_module
*
******************************************************************************/

int walkback_initialize_module(void) { return 0; }

#if defined(__sparc)
/******************************************************************************
*
* Function: __sparc_stack_walkback
*
******************************************************************************/

/* Probe data symbol in libpthread.  If defined, this application
 * has been linked with libpthread.  We use a weak reference.  If the
 * symbol is undefined, its address will be 0.
 */
#pragma weak thr_probe_getfunc_addr
extern void *thr_probe_getfunc_addr;
extern char **_environ;

/* Flush the register window. */
#define FLUSHWIN() __asm("ta 3");

/* Get the return address for this function (the pc of the caller).
   Get the frame pointer for this function (the stack pointer for
   the previous function).
 */
static void __sparc_get_pc_and_sp(/*void *pc, void *sp */)
{
#ifdef _LP64
    __asm("stx %i7, [%i0]; stx %i6, [%i1]");
#else
    __asm("stw %i7, [%i0]; stw %i6, [%i1]");
#endif
}

/* Flush the register windows. */
static void flushWindow() { FLUSHWIN(); }

static struct frame *nextFrame(struct frame *f)
{
#ifdef __sparcv9
    /* add the 'stack bias' */
    return (struct frame *)((char *)(f->fr_savfp) + 2047);
#else
    return f->fr_savfp;
#endif
}

static int __sparc_stack_walkback(ucontext_t *context, unsigned maxframes,
                                  void (*handler)(void *returnaddr,
                                                  void *handlerarg),
                                  void *handlerarg)
{

    struct frame firstframe;
    struct frame *walkframe;
    char *max_frame_address;
    char *max_stack_address;
    char *min_stack_address;
    int i;

    memset(&firstframe, 0, sizeof(firstframe));

    flushWindow();

    if (context != NULL) {
        firstframe.fr_savpc = context->uc_mcontext.gregs[REG_PC];
        firstframe.fr_savfp =
            (struct frame *)context->uc_mcontext.gregs[REG_O6];

        if (&thr_probe_getfunc_addr == 0) {
            /* Single threaded - libptrhead not linked in. */
            min_stack_address = (char *)context->uc_stack.ss_sp;
            max_stack_address = min_stack_address + context->uc_stack.ss_size;
        } else {
            /* libptrhead linked in */
            stack_t thrstack;
            if (thr_stksegment(&thrstack) != 0) {
                return ENOCONTEXT;
            }
            max_stack_address = (char *)thrstack.ss_sp;
            min_stack_address = max_stack_address - thrstack.ss_size;
        }
    } else {
        /* No passed in context. */

        __sparc_get_pc_and_sp(&firstframe.fr_savpc, &firstframe.fr_savfp);

        if (&thr_probe_getfunc_addr == 0) {
            /* Single threaded - libpthread not linked in. */
            /* System puts environment pointers and strings at top of main
             * program stack.  Global variable _environ points to the
             * environment
             * pointers.  This gives us an approximate max stack address.
             */
            min_stack_address = (char *)nextFrame(&firstframe);
            max_stack_address = (char *)_environ;
        } else {
            /* libpthread linked in */
            stack_t thrstack;
            if (thr_stksegment(&thrstack) != 0) {
                return ENOCONTEXT;
            }
            max_stack_address = thrstack.ss_sp;
            min_stack_address = max_stack_address - thrstack.ss_size;
        }
    }

    max_frame_address = (max_stack_address - MIN_FRAME_SIZE);

    if (min_stack_address > max_frame_address) {
        return ESTACKMAX;
    }

    walkframe = &firstframe;

    for (i = 0; i < (int)maxframes; i++) {

        handler((void *)walkframe->fr_savpc, handlerarg);

        walkframe = nextFrame(walkframe);

        if ((char *)walkframe < min_stack_address) {
            return EBELOWMIN;
        }

        if ((char *)walkframe > max_frame_address) {
            return EABOVEMAX;
        }

        if (walkframe->fr_savfp == NULL) {
            break;
        }

        if ((char *)nextFrame(walkframe) <= (char *)walkframe) {
            return ECYCLE;
        }
    }

    return 0;

} /*    end of __sparc_stack_walkback()    */
#endif

#if defined(_AIX)
/******************************************************************************
*
* Function: _AIX_find_traceback_table
* Look in traceback table to determine whether this function saves the
* link register on the stack.
*
******************************************************************************/

static void _AIX_find_traceback_table(void *pc, int *saves_link)
{

    struct tbtable_short *tbtable;
    int *wordptr;

    wordptr = (int *)pc;

    while (*wordptr != 0) {
        ++wordptr;
    }
    ++wordptr;

    tbtable = (struct tbtable_short *)wordptr;
    *saves_link = tbtable->saves_lr;
} /*    end of _AIX_find_traceback_table()    */

/******************************************************************************
*
* Function: _AIX_get_pc_and_sp
*
******************************************************************************/

static void _AIX_get_pc_and_sp(void **pc, void **sp)
{
    int i[1];
    *pc = ((struct frame *)&i[4])->fr_savpc;
    *sp = &i[4];
    return; /* this must be a non-leaf function, so call something... */
    _AIX_get_pc_and_sp(pc, sp);
} /*    end of _AIX_get_pc_and_sp()    */

/******************************************************************************
*
* Function: _AIX_stack_walkback
*
******************************************************************************/

static int _AIX_stack_walkback(ucontext_t *context, unsigned maxframes,
                               void (*handler)(void *returnaddr,
                                               void *handlerarg),
                               void *handlerarg)
{

    struct frame firstframe;
    struct frame *walkframe;
    char *min_stack_address;
    char *max_stack_address;
    void *sp;
    void *pc;
    void *link_register = 0;
    int i;

    if (context != NULL) {
        int saves_link = 0;
        min_stack_address = (char *)context->uc_mcontext.jmp_context.gpr[1];
        firstframe.fr_savpc = (void *)context->uc_mcontext.jmp_context.iar;
        _AIX_find_traceback_table(firstframe.fr_savpc, &saves_link);
        if (saves_link == 0) {
            /* If this function does not save the link register on the stack
             * (for example, a leaf function), we use the value from the
             * context.
             * If the link register is saved, it means the code may re-use it
             * and
             * trash the actual return address.  We must then get it from the
             * stack.
             */
            link_register = (void *)context->uc_mcontext.jmp_context.lr;
            if (link_register == (void *)-1) {
                link_register = 0;
            }
        }
        firstframe.fr_savfp = ((struct frame *)min_stack_address)->fr_savfp;
    } else {
        _AIX_get_pc_and_sp(&pc, &sp);
        firstframe.fr_savpc = pc;
        firstframe.fr_savfp = ((struct frame *)sp)->fr_savfp;
        min_stack_address = (char *)sp;
    }

    walkframe = &firstframe;

    max_stack_address = (char *)&errno - 1; /* thread-specific */

    if (min_stack_address > max_stack_address) {
        return ESTACKMAX;
    }

    for (i = 0; i < maxframes; i++) {

        if ((i == 1) && (link_register != 0)) {
            handler(link_register, handlerarg);
        } else {
            handler((void *)walkframe->fr_savpc, handlerarg);
        }

        walkframe = walkframe->fr_savfp;

        if ((char *)walkframe < min_stack_address) {
            return EBELOWMIN;
        }

        if ((char *)&walkframe[1] > max_stack_address + 1) {
            return EABOVEMAX;
        }

        if (walkframe->fr_savfp == NULL) {
            break;
        }

        if ((char *)walkframe->fr_savfp <= (char *)walkframe) {
            return ECYCLE;
        }
    }

    return 0;

} /*    end of _AIX_stack_walkback()    */
#endif

#if defined(__linux__) || defined(__APPLE__)

/******************************************************************************
*
* Function: __linux_stack_walkback
*
******************************************************************************/

static int __linux_stack_walkback(ucontext_t *context, unsigned maxframes,
                                  void (*handler)(void *returnaddr,
                                                  void *handlerarg),
                                  void *handlerarg)
{
#ifdef USE_UNWIND
    unw_cursor_t cursor;
    unsigned int i;
    unw_word_t ip;
    ucontext_t uc;

    if (context == 0) {
        context = &uc;
        unw_getcontext(context);
    }
    unw_init_local(&cursor, context);
    for (i = 0; i < maxframes; ++i) {
        unw_get_reg(&cursor, UNW_REG_IP, &ip);
        (*handler)((void *)ip, handlerarg);
        if (unw_step(&cursor) <= 0) {
            break;
        }
    }
#endif
    return 0;
}
#endif

#if defined(__hpux)

/* Create a signal safe version of malloc for use by the internal unwind API.
 * The memory is provided by allocator. Pointers returned are aligned to
 * boundary specified by MIN_ALIGNMENT (must be a power of 2).
 */

static void walkback_allocator_init(walkback_alloc_t *allocator)
{
    allocator->cursor = (uintptr_t)allocator->mem_buf % MIN_ALIGNMENT;
    if (allocator->cursor) {
        allocator->cursor = MIN_ALIGNMENT - allocator->cursor;
    }
}

static void *walkback_allocate(walkback_alloc_t *allocator, size_t size)
{

    size_t padding;
    char *p;

    if (allocator->cursor > ALLOCATOR_BUFSIZE) {
        return NULL;
    }

    if (size == 0) {
        size = 1;
    }

    if (size > (ALLOCATOR_BUFSIZE - allocator->cursor)) {
        return NULL;
    }

    p = allocator->mem_buf + allocator->cursor;
    allocator->cursor += size;

    padding = size % MIN_ALIGNMENT;

    if (padding) {
        allocator->cursor += MIN_ALIGNMENT - padding;
    }

    return p;
}

static void walkback_free(walkback_alloc_t *allocator, void *ptr) {}

/******************************************************************************
*
* Function: __hpux_stack_walkback
*
******************************************************************************/

static int __hpux_stack_walkback(ucontext_t *context, unsigned maxframes,
                                 void (*handler)(void *returnaddr,
                                                 void *handlerarg),
                                 void *handlerarg)
{

    struct uwx_self_info *selfinfo;
    struct uwx_config config;
    struct uwx_env *env;
    uint64_t ip;
    int rcode;
    int i;
    int skip_count;
    walkback_alloc_t allocator;

    walkback_allocator_init(&allocator);

    config.flags = UWX_CONFIG_ASYNC | UWX_CONFIG_ALLOC;
    config.alloc = (alloc2_cb)walkback_allocate;
    config.free = (free2_cb)walkback_free;
    config.alloc_parm = &allocator;

    env = uwx_init_config(&config, &rcode);

    if (env == NULL) {
        return EUNWINDERRR(rcode);
    }

    if (context != NULL) {
        skip_count = 0;
    } else {
        skip_count = 3;
    }

    selfinfo = uwx_self_init_info(env);

    if (selfinfo == NULL) {
        uwx_free(env);
        return EUNWINDERRR(UWX_ERR_NOMEM);
    }

    rcode = uwx_register_callbacks(env, (intptr_t)selfinfo, uwx_self_copyin,
                                   uwx_self_lookupip);

    if (rcode != UWX_OK) {
        uwx_self_free_info(selfinfo);
        uwx_free(env);
        return EUNWINDERRR(rcode);
    }

    if (context == NULL) {
        rcode = uwx_self_init_context(env);
    } else {
        rcode = uwx_self_init_from_sigcontext(env, selfinfo, context);
    }

    if (rcode != UWX_OK) {
        uwx_self_free_info(selfinfo);
        uwx_free(env);
        return EUNWINDERRR(rcode);
    }

    for (i = 0; i < (maxframes + skip_count); i++) {

        if (i > 0) {
            rcode = uwx_step(env);

            if (rcode == UWX_BOTTOM) {
                break;
            }

            if (rcode == UWX_ABI_FRAME) {

                if (uwx_get_abi_context_code(env) != 0x101) {
                    break;
                }

                rcode = uwx_self_do_context_frame(env, selfinfo);

                if (rcode != UWX_OK) {
                    break;
                }

                continue;
            }

            if (rcode != UWX_OK) {
                uwx_self_free_info(selfinfo);
                uwx_free(env);
                return EUNWINDERRR(rcode);
            }
        }

        if (i >= skip_count) {
            rcode = uwx_get_reg(env, UWX_REG_IP, &ip);

            if (rcode != UWX_OK) {
                uwx_self_free_info(selfinfo);
                uwx_free(env);
                return EUNWINDERRR(rcode);
            }

            handler((void *)(uintptr_t)ip, handlerarg);
        }
    }

    uwx_self_free_info(selfinfo);
    uwx_free(env);

    return 0;

} /*    end of __hpux_stack_walkback()    */

#endif

/******************************************************************************
*
* Function: stack_pc_walkback
*
******************************************************************************/

int stack_pc_walkback(ucontext_t *context, /* or NULL for current context */
                      unsigned maxframes,
                      void (*handler)(void *returnaddr, void *handlerarg),
                      void *handlerarg)
{

#if defined(__sparc)

    return __sparc_stack_walkback(context, maxframes, handler, handlerarg);

#elif defined(_AIX)

    return _AIX_stack_walkback(context, maxframes, handler, handlerarg);

#elif defined(__linux__) || defined(__APPLE__)

    return __linux_stack_walkback(context, maxframes, handler, handlerarg);

#elif defined(__hpux)

    return __hpux_stack_walkback(context, maxframes, handler, handlerarg);

#else

#error Unsupported architecture

#endif

} /*    end of stack_pc_walkback()    */

/******************************************************************************
*
* Function: stack_pc_walkback_print
*
******************************************************************************/

void stack_pc_walkback_print(void *returnaddr, void *arg)
{

    fprintf(stderr, "0x%p\n", returnaddr);
} /*    end of stack_pc_walkback_print()    */

/******************************************************************************
*
* Function: walkback_strerror
*
******************************************************************************/

void walkback_strerror(int rcode, char *errormsg, unsigned maxerrormsgsize)
{

    const char *msg;
    char buf[80];

    switch ((int8_t)rcode) {

    case 0:
        msg = "success";
        break;

    case ENOCONTEXT:
        msg = "getcontext() failed";
        break;

    case ECYCLE:
        msg = "stack corrupted: frame cycle";
        break;

    case EBELOWMIN:
        msg = "stack corrupted: frame below the stack";
        break;

    case EABOVEMAX:
        msg = "stack corrupted: frame above the stack";
        break;

    case ENOIMPL:
        msg = "walkback not implemented";
        break;

    case ESTACKMAX:
        msg = "max stack address not found";
        break;

    case EUNWINDER:
        snprintf(buf, sizeof(buf), "line %d: error %d in system stack unwinder",
                 EUNWINDLINE(rcode), EUNWINDCODE(rcode));
        msg = buf;
        break;

    case EOPEN:
        msg = "error creating lock file";
        break;

    case ECLOSE:
        msg = "error closing lock file";
        break;

    case EUNLINK:
        msg = "error deleting lock file";
        break;

    case EGBLCORRUPT:
        msg = "corrupted global memory";
        break;

    case EUNINIT:
        msg = "module is not initialized";
        break;

    default:
        msg = "unclassified error";
        break;
    }

    if (maxerrormsgsize > 0) {
        *errormsg = '\0';
        strncat(errormsg, msg, maxerrormsgsize - 1);
    }

} /*    end of walkback_strerror()    */

/******************************************************************************
*
* Function: pclist_add
*
******************************************************************************/

static void pclist_add(void *address, void *arg)
{

    struct pclist_add_arg_t *input = (struct pclist_add_arg_t *)arg;

    if (input->pcOutCount < input->pcMaxCount) {
        input->pcArray[input->pcOutCount] = address;
        input->pcOutCount++;
    }

} /*    end of pclist_add()    */

/******************************************************************************
*
* Function: stack_pc_getlist
*
******************************************************************************/

int /* rcode */
    stack_pc_getlist(
        ucontext_t *context,  /* or NULL for current context */
        void **pcArray,       /* output array of program counters */
        unsigned pcArraySize, /* number of elements in pcArray */
        unsigned *pcOutCount  /* number of program counters returned */
        )
{

    struct pclist_add_arg_t arg;
    int rcode = ENOIMPL, stackmin;

    arg.pcArray = pcArray;
    arg.pcMaxCount = pcArraySize;
    arg.pcOutCount = 0;

    if (gbl_walkback_enabled) {
        rcode = stack_pc_walkback(context, pcArraySize, pclist_add, &arg);
    }

    *pcOutCount = arg.pcOutCount;

    stackmin = pcArraySize < arg.pcOutCount ? pcArraySize : arg.pcOutCount;
    trace_pc_getlist(pcArray, stackmin, stderr);
    return rcode;
} /*    end of stack_pc_getlist()    */

#define MAXFRAMES 100

void comdb2_cheapstack(FILE *f)
{
    void *stack[MAXFRAMES];
    unsigned int nframes;
    int i;

    if (stack_pc_getlist(NULL, stack, MAXFRAMES, &nframes)) {
        fprintf(f, "Can't get stack trace\n");
        return;
    }
    fprintf(f, "Run showfunc.tsk <comdb2-task> ");
    for (i = 0; i < nframes && i < MAXFRAMES; i++) {
        if (stack[i])
            fprintf(f, "%p ", stack[i]);
    }
    fprintf(f, "\n");
}

int comdb2_cheapstack_char_array(char *str, int maxln)
{
    void *stack[MAXFRAMES];
    unsigned int nframes;
    char *p;
    int i, ccount, first = 1;

    if (maxln <= 0 || stack_pc_getlist(NULL, stack, MAXFRAMES, &nframes)) {
        return -1;
    }
    p = str;
    for (i = 0; i < nframes && i < MAXFRAMES && maxln > 0; i++) {
        if (stack[i]) {
            if (first) {
                ccount = snprintf(p, maxln, "%p", stack[i]);
                first = 0;
            } else {
                ccount = snprintf(p, maxln, " %p", stack[i]);
            }
            p += ccount;
            maxln -= ccount;
        }
    }
    return 0;
}
