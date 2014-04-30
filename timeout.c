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

#define THREAD_EQL(t1,t2) ( ((t1) && (t2) && pthread_equal((t1)->thread_id,(t2)->thread_id)) \
                             || ((t1) == NULL && (t2) == NULL) )
#define SEC_TO_USEC(s) ((uint64_t)(s) * (uint64_t)1000000)
#define USEC_TO_SEC(us) ((uint64_t)(us) / (uint64_t)1000000)

#define TV_TO_USEC(tv) (SEC_TO_USEC((tv)->tv_sec) + (tv)->tv_usec % (uint64_t)1000000)
#define USEC_TO_TV(tv,us) do { (tv)->tv_sec = USEC_TO_SEC(us); \
                               (tv)->tv_usec = (uint64_t)(us) % (uint64_t)1000000; \
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
static struct timeout_event_list pending, timeouts, freelist;
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

static void
dump_list(const char *name, struct timeout_event_list *list)
{
    timeout_event_t **tp;
    unsigned count = 0;
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH(list,tp) count++;
    ANN(pthread_mutex_unlock(&timeout_mutex));
}

#if 0
static void
mc_timeout_free(timeout_event_t **tp)
{
    mutex_lock(&timeout_mutex);
    mc_timeout_free_no_mutex(tp);
    mutex_unlock(&timeout_mutex);
}
#endif

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
            else if(list == &timeouts)
                list_name = "timeouts";
            else if(list == &freelist)
                list_name = "freelist";
            fprintf(stderr,"Added new timeout at %llu ms to %s list.\n",
                        TV_TO_USEC(&t->tv) / 1000, list_name);
        }
    }
    ANN(pthread_mutex_unlock(&timeout_mutex));
    return t;
}

/* Exported for visibility to thread.c */
void mc_timeout_check_idle(LIBEVENT_THREAD *me)
{
    timeout_event_t *t, **tp;
    uint64_t now = SEC_TO_USEC(current_time);

    if(settings.verbose > 2)
        fprintf(stderr,"checkout idle timeout for a thread (current_time = %u)\n",
                            (unsigned)current_time);
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_FOREACH_E(&pending,tp,t) {
        if (t->fd > -1 && conns[t->fd] && THREAD_EQL(conns[t->fd]->thread,me)) {
            /* Make sure we are still talking about the same connection and
             * that it has really timed out.
             */
            if(t->c == conns[t->fd] && CONN_CAN_TIMEOUT(t->c)) {
                uint64_t elapsed = now - SEC_TO_USEC(t->c->last_cmd_time);
                if(elapsed >= TV_TO_USEC(&t->tv)) {
                    uint64_t otv = TV_TO_USEC(&t->tv);
                    event_del(&t->ev);
                    LL_REMOVE(tp);
                    USEC_TO_TV(&t->tv,elapsed);
                    conn_timeout(t->c,&t->tv); /* memcached.c */
                    if (t->type == timeout_type_custom) {
                        USEC_TO_TV(&t->tv,otv);
                        event_add(&t->ev,&t->tv);
                    } else
                        TIMEOUT_FREE_UNSAFE(t);
                } else if(t->type == timeout_type_default) {
                    if(settings.verbose)
                        fprintf(stderr,"Destroying pending timeout as it seems to have reset\n");
                    TIMEOUT_DESTROY_UNSAFE(tp);
                }
            } else
                /* a mismatch between connections or file descriptors
                 * means the connection is different than when the timeout
                 * was registered. Silently discard it.
                 */
                TIMEOUT_DESTROY_UNSAFE(tp);

        } else
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
    if(!me || THREAD_EQL(me,t->c->thread)) {
        fprintf(stderr, "warning: not running in connection thread, "
                             "redirecting to timeout_event_handler\n");
        timeout_event_handler(fd,flags,arg);
    } else
        mc_timeout_check_idle(me);
}

/* NB: timeout_event_handler() runs in the timeout background thread */
static void timeout_event_handler(const int fd, const short flags, void *arg)
{
    timeout_event_t *t = (timeout_event_t*)arg;
    timeout_event_t *cause = NULL;
    char buf[1] = {'T'};
    uint64_t next_sleep = (uint64_t)settings.timeout_thread_sleep;
    uint64_t idle_timeout = SEC_TO_USEC(settings.idle_timeout);
    uint64_t now = SEC_TO_USEC(current_time);
    struct timeval tv;
    uint64_t elapsed;
    int i;
    LIBEVENT_THREAD *wake_thread = NULL;

    AN(t);
    if (max_fds <= 0)
        max_fds = settings.maxconns;

    ANN(pthread_mutex_lock(&timeout_mutex));
    if(settings.verbose > 1)
        fprintf(stderr,"\ntimeout_event_handler thread start, now = %llu\n",USEC_TO_SEC(now));

    if(!LL_EMPTY(&timeouts)) {
        timeout_event_t *ti, **tp;
        /* scan the list of custom/old timeouts to move to pending or destroy */
        LL_FOREACH_E(&timeouts,tp,ti) {
            if(ti->fd == fd || fd == -1) {
                if(cause == NULL && ti->fd > -1 && ti->fd < max_fds &&
                                                conns[ti->fd] && ti->c) {
                    elapsed = now - SEC_TO_USEC(ti->c->last_cmd_time);
                    if (!CONN_CAN_TIMEOUT(ti->c) || ti->c != conns[ti->fd]) {
                        ti->fd = -1;
                        ti->c = NULL;
                    } else if (elapsed >= TV_TO_USEC(&t->tv)) {
                        cause = ti;
                        event_del(&cause->ev);
                        /* move the timeout to the pending list and wake up
                         * the thread responsible.
                         */
                        wake_thread = cause->c->thread;
                        AN(wake_thread && wake_thread->notify_send_fd > -1);
                        if(write(wake_thread->notify_send_fd,buf,1) == 1) {
                            LL_REMOVE(tp);
                            USEC_TO_TV(&cause->tv,elapsed);
                            LL_INSERT(&pending,cause);
                        } else {
                            wake_thread = NULL;
                            perror("Failed writing timeout to notify pipe");
                            TIMEOUT_DESTROY_UNSAFE(tp);
                        }
                        continue;
                    }
                } else {
                    /* An old timeout has been fully handled (fd == -1) or
                       the connection is invalid, either way discard it */
                    ti->fd = -1;
                    ti->c = NULL;
                }
            }
            if(ti->fd == -1)
                TIMEOUT_DESTROY_UNSAFE(tp);
        }
    }
    if(settings.verbose > 2)
        fprintf(stderr,"timeout_event_handler existing scan complete\n");
    ANN(pthread_mutex_unlock(&timeout_mutex));

    /* Scan all connections to see if there are any candidates for timeout,
     * if so, move them to pending and wake up the appropriate thread.
     * We only wake up one thread per loop, although all candidates for that
     * thread will get moved in one pass.
     */
    for (i = 0; i < max_fds; i++) {
        conn *c = conns[i];
        if(!c || !CONN_CAN_TIMEOUT(c) || !c->thread)
            continue;
        printf("last_cmd_time=%llu\n",SEC_TO_USEC(c->last_cmd_time));
        if((elapsed = now - SEC_TO_USEC(c->last_cmd_time)) >= idle_timeout) {
            if (!wake_thread || THREAD_EQL(wake_thread,c->thread)) {
                USEC_TO_TV(&tv,elapsed);
                t = mc_timeout_add(c,&tv,0,&pending,NULL,timeout_close_handler);
                AN(t);
                assert(c->thread->notify_send_fd > -1);
                if (write(c->thread->notify_send_fd,buf,1) != 1) {
                    timeout_event_t **tp;
                    perror("Failed writing timeout to notify pipe");
                    USEC_TO_TV(&tv,settings.timeout_thread_sleep);
                    AZ(pthread_mutex_lock(&timeout_mutex));
                    LL_FINDP(&pending,tp,t);
                    AN(tp);
                    LL_INSERT(&timeouts,LL_REMOVE(tp));
                    ANN(pthread_mutex_unlock(&timeout_mutex));
                } else
                    wake_thread = c->thread;
            } else if(wake_thread)
                /* Another thread needs to be woken up, so schedule an
                 * immediate re-run.
                 */
                 next_sleep = 0;
        } else if(elapsed < idle_timeout && next_sleep > idle_timeout - elapsed)
            next_sleep = idle_timeout - elapsed;
    }

    if(freelist_len >= MAX_FREE_TIMEOUTS) {
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

    if(settings.verbose > 2) {
        static int counter = 0;

        if(counter % 10 == 0) {
            dump_list("timeouts",&timeouts);
            dump_list("pending",&pending);
            dump_list("freelist",&freelist);
        }
        counter++;
    }
    USEC_TO_TV(&t->tv,next_sleep);
    event_add(&t->ev,(next_sleep > 0 ? &t->tv : NULL));
    if(settings.verbose > 2)
        fprintf(stderr,"timeout_event_handler thread run complete, sleeping for %llu ms\n\n",
                next_sleep/1000);

}

/****************************************************************************/
/* Timeout thread entry point                                               */
/****************************************************************************/
static void*
timeout_thread(void *c)
{
    AN(c);
    ANN(pthread_mutex_lock(&timeout_mutex));
    LL_INIT(&timeouts);
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
    LL_FOREACH(&timeouts,tp) {
        if((*tp)->c == c) {
            /* the timeout thread will clear these up later */
            (*tp)->fd = -1;
            (*tp)->c = NULL;
        }
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
