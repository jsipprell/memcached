/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */

/* A pipe-based version of timeouts for older libevents which don't support
 * safely calling event_*() from another thread to awaken them.
 */

#include "memcached.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>

#ifndef NEED_LL_DECL
#define NEED_LL_DECL 1
#endif
#include "timeout.h"

typedef uint64_t usec_t;
#define USEC_T(us) ((usec_t)(us))
#define THREAD_EQL(t1,t2) ( ((t1) && (t2) && pthread_equal((t1)->thread_id,(t2)->thread_id)) \
                             || ((t1) == NULL && (t2) == NULL) )
#define SEC_TO_USEC(s) (USEC_T(s) * USEC_T(1000000))
#define SEC_TO_MSEC(s) (USEC_T(s) * USEC_T(1000))
#define USEC_TO_SEC(us) (USEC_T(us) / USEC_T(1000000))
#define USEC_TO_MSEC(us) (USEC_T(us) / USEC_T(1000))
#define TV_TO_USEC(tv) (SEC_TO_USEC((tv)->tv_sec) + (tv)->tv_usec % USEC_T(1000000))
#define USEC_TO_TV(tv,us) do { (tv)->tv_sec = USEC_TO_SEC(us); \
                               (tv)->tv_usec = USEC_T(us) % USEC_T(1000000); \
                             } while(0)
#define STATE_CAN_TIMEOUT(s) ((s) > conn_listening && \
                              (s) != conn_closing && \
                              (s) < conn_closed)
#define CONN_CAN_TIMEOUT(c) ((c)->last_cmd_time && \
                             !IS_UDP((c)->transport) && (c)->sfd > -1 && \
                             STATE_CAN_TIMEOUT((c)->state))

#ifndef STRINGIFY
#define STRINGIFY(x) #x
#endif

#define ANN(cond) do { \
                    if((cond) == -1) { \
                        vperror("%s:%d",__FILE__,__LINE__); \
                        abort(); \
                  } } while(0)

#ifndef NDEBUG
#define AZ(cond) assert((cond) == 0)
#define AN(cond) assert((cond) != 0)
#else
#define AZ(cond) (void)(cond)
#define AN(cond) (void)(cond)
#endif

LL_DECL(timeout_event) {
    enum {
        timeout_type_default = 0,
        timeout_type_custom
    } type;
    struct event ev;
    struct timeval tv;
    int fd;
    conn *c;
    LL_LINK(timeout_event);
} timeout_event_t;
#define LL_REMOVE(lp) LL_REMOVE_ELEM(lp,timeout_event)

typedef void cbfn(const int, const short, void *);

/* locals */
static pthread_once_t timeout_init_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t timeout_mutex;
static pthread_key_t current_thread_key;
static struct timeout_event_list pending, freelist;
static unsigned freelist_len = 0;
static timeout_event_t wakeup_timeout;
static struct event_base *timeout_event_base = NULL;
static int max_fds = -1;

/* foward decls */
static cbfn timeout_event_handler;
static unsigned mc_timeout_destroy_no_mutex(timeout_event_t**);
static unsigned mc_timeout_free_no_mutex(timeout_event_t*);

/* mc_timeout_destroy_no_mutex() and mc_timeout_free_no_mutex() should only
 * be called while timeout_mutex is held.
 */
static unsigned
mc_timeout_destroy_no_mutex(timeout_event_t **tp)
{
    AN(tp); AN(*tp);
    event_del(&(*tp)->ev);
    LL_INSERT(&freelist,LL_REMOVE(tp));
#ifndef NDEBUG
    if(settings.verbose > 2)
        fprintf(stderr, "timeout added to freelist, count=%u\n", freelist_len+1);
#endif

    return ++freelist_len;
}
#define TIMEOUT_DESTROY_UNSAFE mc_timeout_destroy_no_mutex

static unsigned
mc_timeout_free_no_mutex(timeout_event_t *t)
{
    AN(t);
    LL_INSERT(&freelist,t);
#ifndef NDEBUG
    if(settings.verbose > 1)
        fprintf(stderr, "timeout added to freelist, count=%u\n", freelist_len+1);
#endif
    return ++freelist_len;
}
#define TIMEOUT_FREE_UNSAFE mc_timeout_free_no_mutex

#ifndef NDEBUG
static void
dump_list(const char *name, struct timeout_event_list *list)
{
    timeout_event_t **tp;
    unsigned count = 0;
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH(list,tp) count++;
    ANN(pthread_mutex_unlock(&timeout_mutex));
}
#endif

static void
mc_timeout_destroy(struct timeout_event_list *list, timeout_event_t *t)
{
    timeout_event_t **tp;
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH(list,tp) {
        if(*tp == t) {
            LL_REMOVE(tp);
            break;
        }
    }
    if(event_initialized(&t->ev))
        event_del(&t->ev);
    mc_timeout_free_no_mutex(t);
    ANN(pthread_mutex_unlock(&timeout_mutex));
}

static timeout_event_t*
mc_timeout_find(struct timeout_event_list *list, int fd)
{
    timeout_event_t **tp,*t = NULL;

    ANN(pthread_mutex_lock(&timeout_mutex));
    if (!LL_EMPTY(list)) {
        LL_FOREACH(list,tp)
            if(*tp && (*tp)->fd == fd) {
                t = *tp;
                break;
            }
    }
    ANN(pthread_mutex_unlock(&timeout_mutex));
    return t;
}

static timeout_event_t*
mc_timeout_add(conn *c, const struct timeval *tv, short flags,
                           struct timeout_event_list *list,
                        struct event_base *base, cbfn *fn)
{
    timeout_event_t *t;
    /* add a new timeout to the list for a given connection.
    The timeout may be immediate or in the future. */
    AN(c);
    ANN(pthread_mutex_lock(&timeout_mutex));
    if (!LL_EMPTY(&freelist)) {
        timeout_event_t **tp = LL_FIRST(&freelist);
        AN(tp); AN(*tp);
        assert(freelist_len > 0);
        freelist_len--;
        t = LL_REMOVE(tp);
        if(settings.verbose > 2)
            fprintf(stderr,"Used an old timeout struct, freelist size is %u\n",freelist_len);
    } else
        t = (timeout_event_t*)calloc(1,sizeof(timeout_event_t));
    AN(t);
    t->c = c;
    t->fd = (c ? c->sfd : -1);
    if(tv) {
        t->tv.tv_sec = tv->tv_sec;
        t->tv.tv_usec = tv->tv_usec;
    } else {
        t->tv.tv_sec = 0;
        t->tv.tv_usec = 0;
    }
    event_set(&t->ev,t->fd,flags,(fn ? fn : timeout_event_handler),t);
    event_base_set((base ? base : timeout_event_base),&t->ev);
    if (list) {
        LL_INSERT(list,t);
        if(settings.verbose > 2) {
            const char *list_name = "unknown";
            if(list == &pending)
                list_name = "pending";
            else if(list == &freelist)
                list_name = "freelist";
            fprintf(stderr,"%d: added new timeout at %llu ms to %s list.\n",
                t->fd,(unsigned long long)(TV_TO_USEC(&t->tv) / USEC_T(1000)),
                                                                    list_name);
        }
    }
    ANN(pthread_mutex_unlock(&timeout_mutex));
    return t;
}

/* Exported for visibility to thread.c */
void mc_timeout_check_idle(LIBEVENT_THREAD *me)
{
    timeout_event_t *t, **tp;
    usec_t now = SEC_TO_USEC(current_time);

    if(settings.verbose > 2)
        fprintf(stderr,"idle timeout connection scan, clock:%llu\n",
                        (unsigned long long)SEC_TO_MSEC(current_time));
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH_E(&pending,tp,t) {
        if (t->fd > -1 && conns[t->fd] && THREAD_EQL(conns[t->fd]->thread,me)) {
            /* Make sure we are still talking about the same connection and
             * that it has really timed out.
             */
            if(t->c->sfd == t->fd && CONN_CAN_TIMEOUT(t->c)) {
                usec_t elapsed = now - SEC_TO_USEC(t->c->last_cmd_time);
                if(elapsed >= TV_TO_USEC(&t->tv)) {
                    event_del(&t->ev);
                    LL_REMOVE(tp);
                    if(settings.verbose)
                        fprintf(stderr,"%d: idle disconnect (%llu.%llu seconds)\n",t->fd,
                            (unsigned long long)USEC_TO_SEC(elapsed),
                            (unsigned long long)USEC_TO_MSEC(elapsed));
                    conn_timeout(t->c,&t->tv,elapsed); /* memcached.c */
                    if (t->type == timeout_type_custom)
                        event_add(&t->ev,&t->tv);
                    else
                        TIMEOUT_FREE_UNSAFE(t);
                } else if(t->type == timeout_type_default) {
                    if(settings.verbose)
                        fprintf(stderr,"%d: destroying pending timeout as it "
                                            "seems to have reset\n",t->fd);
                    TIMEOUT_DESTROY_UNSAFE(tp);
                }
            } else
                /* a mismatch between file descriptors means the connection is
                 * different than when the timeout was registered. Silently discard it.
                 */
                TIMEOUT_DESTROY_UNSAFE(tp);

        } else
            /* The connection no longer exists or there is a thread mismatch.
             * ignore it.
             */
            TIMEOUT_DESTROY_UNSAFE(tp);
    }
    ANN(pthread_mutex_unlock(&timeout_mutex));
}

/* timeout_close_handler should only be called from the libevent
 * event_base of the thread owning a connection which registered
 * a custom timeout.
 */
static void
timeout_close_handler(const int fd, const short flags, void *arg)
{
    LIBEVENT_THREAD *me = pthread_getspecific(current_thread_key);
    timeout_event_t *t = (timeout_event_t*)arg;

    if(settings.verbose > 2)
        fprintf(stderr, "timeout handler on thread running for fd %d\n", fd);
    AN(t);
    AN(t->c);
    if(!me || !THREAD_EQL(me,t->c->thread)) {
        fprintf(stderr, "warning: not running in connection thread, "
                             "redirecting to timeout_event_handler\n");
        timeout_event_handler(fd,flags,arg);
    } else
        mc_timeout_check_idle(me);
}

static inline
int thr_find(LIBEVENT_THREAD *threads[], LIBEVENT_THREAD *td)
{
    int i;

    for(i = 0; i < settings.num_threads; i++)
        if(THREAD_EQL(threads[i],td))
            break;

    return (threads[i] != NULL ? i : -1);
}

static inline
int thr_add(LIBEVENT_THREAD *threads[], LIBEVENT_THREAD *td)
{
    int i;

    for(i = 0; i < settings.num_threads && threads[i] && !THREAD_EQL(threads[i],td); i++)
        ;

    if(i < settings.num_threads && !threads[i])
        threads[i] = td;
    else
        i = -1;
    return i;
}

/* NB: timeout_event_handler() runs in the timeout background thread */
static
void timeout_event_handler(const int fd, const short flags, void *arg)
{
    timeout_event_t *t = (timeout_event_t*)arg;
    char buf[1] = {'T'};
    usec_t next_sleep = (usec_t)settings.timeout_thread_sleep;
    usec_t idle_timeout = SEC_TO_USEC(settings.idle_timeout);
    usec_t now = SEC_TO_USEC(current_time);
    struct timeval tv;
    usec_t elapsed;
    int i;
    static LIBEVENT_THREAD **threads = NULL;
    static int nthr = -1;

    AN(t);

    if(nthr != settings.num_threads || !threads) {
        nthr = settings.num_threads;
        threads = calloc(nthr+1,sizeof(*threads));
        if (threads == NULL) {
            vperror("calloc while allocating memory for awakened threads in %s:%d",
                     __FILE__,__LINE__);
            event_add(&t->ev,&t->tv);
            return;
        }
    } else for(i = 0; i < nthr; i++)
        threads[i] = NULL;

    if (max_fds <= 0)
        max_fds = settings.maxconns;

    if(settings.verbose > 1)
        fprintf(stderr,"\ntimeout_event_handler thread start, now = %llu\n",
                                        (unsigned long long)USEC_TO_SEC(now));

    /* Scan all connections to see if there are any candidates for timeout,
     * if so, add a timeout to pending and wake up the appropriate thread.
     * We only wake up one thread per loop, although all candidates for that
     * thread will get moved in one pass.
     */
    for (i = 0; i < max_fds; i++) {
        conn *c = conns[i];
        if(!c || !CONN_CAN_TIMEOUT(c) || !c->thread || c->last_cmd_time == 0)
            continue;
        if((elapsed = now - SEC_TO_USEC(c->last_cmd_time)) >= idle_timeout) {
            if (mc_timeout_find(&pending,c->sfd))
                continue;
            USEC_TO_TV(&tv,elapsed);
            t = mc_timeout_add(c,&tv,0,&pending,NULL,timeout_close_handler);
            AN(t);
            if(thr_find(threads,c->thread) == -1) {
                assert(c->thread->notify_send_fd > -1);
                if (write(c->thread->notify_send_fd,buf,1) != 1) {
                    perror("Failed writing timeout to notify pipe");
                    mc_timeout_destroy(&pending,t);
                } else
                    thr_add(threads,c->thread);
            }
        } else if(elapsed < idle_timeout && next_sleep > idle_timeout - elapsed)
            next_sleep = idle_timeout - elapsed;
    }

    /* Cleanup freelist if its too large */
    if (freelist_len >= MAX_FREE_TIMEOUTS) {
        timeout_event_t **tp;
        ANN(pthread_mutex_lock(&timeout_mutex));
        for(tp = LL_FIRST(&freelist); *tp != NULL && freelist_len > MAX_FREE_TIMEOUTS / 2;
                                             freelist_len--, tp = LL_FIRST(&freelist))
            free(LL_REMOVE(tp));
        ANN(pthread_mutex_unlock(&timeout_mutex));
    }

    if((t = (timeout_event_t*)arg) != &wakeup_timeout) {
        t = &wakeup_timeout;
        event_del(&t->ev);
    }

#ifndef NDEBUG
    if(settings.verbose > 2) {
        static int counter = 0;

        if(counter % 10 == 0) {
            dump_list("pending",&pending);
            dump_list("freelist",&freelist);
        }
        counter++;
    }
#endif
    USEC_TO_TV(&t->tv,next_sleep);
    event_add(&t->ev,&t->tv);
    if(settings.verbose > 2)
        fprintf(stderr,"timeout_event_handler thread run complete, sleeping for %llu ms\n\n",
                                            (unsigned long long)USEC_TO_MSEC(next_sleep));

}

/****************************************************************************/
/* Timeout thread entry point                                               */
/****************************************************************************/
static void*
timeout_thread(void *c)
{
    AN(c);
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_INIT(&freelist);
    LL_INIT(&pending);
    AN(timeout_event_base = event_init());
    USEC_TO_TV(&wakeup_timeout.tv,settings.timeout_thread_sleep);
    wakeup_timeout.type = timeout_type_default;
    wakeup_timeout.fd = -1;
    wakeup_timeout.c = NULL;
    event_set(&wakeup_timeout.ev,-1,0,timeout_event_handler,&wakeup_timeout);
    event_base_set(timeout_event_base,&wakeup_timeout.ev);
    ANN(pthread_cond_signal((pthread_cond_t*)c));
    ANN(pthread_mutex_unlock(&timeout_mutex));

    ANN(event_add(&wakeup_timeout.ev,&wakeup_timeout.tv));
    event_base_loop(timeout_event_base,0);
    return NULL;
}

static void
timeout_init(void)
{
    static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    static pthread_attr_t ta;
    static pthread_t t;

    AZ(timeout_event_base);
    ANN(pthread_mutex_init(&timeout_mutex,NULL));
    ANN(pthread_mutex_lock(&timeout_mutex));
    ANN(pthread_key_create(&current_thread_key,NULL));
    ANN(pthread_setspecific(current_thread_key,NULL));
    ANN(pthread_attr_init(&ta));
    ANN(pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED));
    ANN(pthread_create(&t,&ta,timeout_thread,&cond));
    while(timeout_event_base == NULL)
        ANN(pthread_cond_wait(&cond,&timeout_mutex));
    ANN(pthread_mutex_unlock(&timeout_mutex));
}

/* Allow a connection to register a timeout other than the default.
 *  when arg should be in microseconds. Returns -1 on failure
 */
int mc_timeout_set(conn *c, unsigned int when)
{
    struct timeval tv;
    timeout_event_t *t;

    if(!timeout_event_base)
        assert(pthread_once(&timeout_init_once,timeout_init));

    AN(c);
    AN(timeout_event_base);

    if(!c->thread || pthread_setspecific(current_thread_key,c->thread) == -1)
        return -1;
    USEC_TO_TV(&tv,when);
    t = mc_timeout_add(c,&tv,EV_PERSIST,&pending,c->thread->base,timeout_close_handler);
    if (t != NULL) {
        t->type = timeout_type_custom;
        event_add(&t->ev,&t->tv);
        return 0;
    }
    return -1;
}

/* Clear all timeouts for a given connection */
void mc_timeout_clear(conn *c)
{
    timeout_event_t **tp;

    if(!timeout_event_base)
        assert(pthread_once(&timeout_init_once,timeout_init));

    AN(timeout_event_base);

    if(c->thread)
        ANN(pthread_setspecific(current_thread_key,c->thread));

    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH(&pending,tp) {
        if((*tp)->c == c)
            TIMEOUT_DESTROY_UNSAFE(tp);
    }
    ANN(pthread_mutex_unlock(&timeout_mutex));
}

/* Initialize maxfds and start timeout bg thread. *
 * This function can be called multiple times, once from the main thread
 * during initialization and once for each worker thread. When called from
 * worker thread initialization pass the thread id but max_conns as -1.
 * Passing thread id as NULL indicates the main thread.
 */
void mc_timeout_init(int max_conns, LIBEVENT_THREAD *t)
{
    if(max_conns > max_fds)
        max_fds = max_conns;

    if(pthread_once(&timeout_init_once,timeout_init) == -1) {
        perror("pthread_once failed while initializing timeout thread");
        abort();
    }

    if(t != NULL)
        ANN(pthread_setspecific(current_thread_key,t));
}
