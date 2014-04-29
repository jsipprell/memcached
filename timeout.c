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
#include <sys/queue.h>

#define THREAD_EQL(t1,t2) ( ((t1) && (t2) && pthread_equal((t1)->thread_id,(t2)->thread_id)) \
                             || (t1 == NULL && t2 == NULL) )
#define SEC_TO_USEC(s) ((uint64_t)(s) * (uint64_t)1000000)
#define USEC_TO_SEC(us) ((uint64_t)(us) / (uint64_t)1000000)

#define TV_TO_USEC(tv) (SEC_TO_USEC((tv)->tv_sec) + (tv)->tv_usec / (uint64_t)1000000)
#define USEC_TO_TV(tv,us) do { (tv)->tv_sec = USEC_TO_SEC(us); \
                               (tv)->tv_usec = (uint64_t)(us) % (uint64_t)1000000; \
                             } while(0)
#define STATE_CAN_TIMEOUT(s) ((s) > conn_listening && \
                              (s) != conn_waiting && \
                              (s) != conn_closing && \
                              (s) < conn_closed)
#define CONN_CAN_TIMEOUT(c) (!IS_UDP(c) && (c)->fd > -1 && \
                             STATE_CAN_TIMEOUT((c)->state))

#ifndef NDEBUG
#define AZ(cond) assert((void*)(cond) == (void*)0)
#define AN(cond) assert((void*)(cond) != (void*)0)
#else
#define AZ(cond) do { if((cond) != 0) { perror(__file__); abort(); }} while(0)
#define AN(cond) (cond)
#endif

typedef struct timeout_event {
    LIST_ENTRY(timeout_event) link;
    struct event ev;
    struct timeval tv;
    int fd;
    conn *c;
} timeout_event_t;

LIST_HEAD(timeout_list, timeout_event);

/* imported globals */
extern struct settings settings;
extern stats stats;
extern conn **conns;
extern volatile rel_time_t current_time;

extern void conn_timeout(conn*,struct timeval*);

typedef cbfn(const int, const short, void *);

/* locals */
static pthread_once_t timeout_init_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t timeout_list_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t current_thread_key;
static struct timeout_list pending;
static struct timeout_list timeouts;
static struct timeout_list freelist;
static unsigned freelist_len = 0;
static timeout_event_t wakeup_timeout;
static volatile event_base *timeout_event_base = NULL;
static int max_fds = -1;

static unsigned timeout_free_unsafe(timeout_event_t *t)
{
    LIST_REMOVE(t, link);
    LIST_INSERT_HEAD(&freelist, t, link);
    return ++freelist_len;
}

static void
timeout_free(timeout_event_t *t)
{
    AN(t);
    AZ(pthread_mutex_lock(&timeout_list_mutex));
    timeout_free_unsafe(t);
    AZ(pthread_mutex_unlock(&timeout_list_mutex));
}

static timeout_event_t*
timeout_add_ex(conn *c, const struct timeval *tv, short flags,
                        struct timeout_list *list,
                        struct event_base *base, cbfn *fn)
{
    timeout_event_t *t;

    /* add a new timeout to the list for a given connection.
    The timeout may be immediate or in the future. */
    AN(c);
    AZ(pthread_mutex_lock(&timeout_list_mutex));
    if (freelist.lh_first != NULL) {
        t = freelist.lh_first;
        freelist_len--;
        LIST_REMOVE(t,link);
    } else
        t = (timeout_event_t*)calloc(1,sizeof(timeout_event_t));
    AN(t);
    t->c = c;
    t->fd = (fd > -2 ? fd : (c ? c->sfd : -1));
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
        LIST_INSERT_HEAD(list,t,link);
        if (list == &freelist) {
            freelist_len++;
        }
    }
    AZ(pthread_mutex_unlock(&timeout_list_mutex));
    return t;
}

inline static timeout_event_t*
timeout_add(conn *c, const struct timeval *tv)
{
    return timeout_add_ex(c,tv,0,&timeouts,NULL,NULL);
}

/* Exported for visibility to thread.c */
void timeout_check_idle(LIBVENT_THREAD *me)
{
    timeout_event_t *t, *next;
    struct timeval tv;
    conn *c;

    AZ(pthread_mutex_lock(&timeout_list_mutex));
    for(t = pending.lh_first; t != NULL && (next = t->le_next) == next; t = next) {
        if (conns[t->fd] && THREAD_EQL(conns[t->fd]->thread,me)) {
            /* Make sure we are still talking about the same connection and
             * that it has really timed out.
             */
            if(t->c == conns[t->fd] && CONN_CAN_TIMEOUT(t->c)) {
                rel_time_t last = current_time - t->c->last_cmd_time;
                event_del(&t->ev);
                LIST_REMOVE(t,link);
                if(last >=  settings.idle_timeout) {
                    t->tv.tv_sec = last - t->c->last_cmd_time;
                    t->tv.tv_usec = 0;
                    conn_timeout(t->c,&t->tv); /* memcached.c */
                    timeout_free_unsafe(t);
                } else {
                    LIST_INSERT_HEAD(t,timeouts.lh_first);
                    /* TODO: wakeup timeout thread */
                }
            } else if(t->c != conns[t->fd]) {
                /* a mismatching between connections or file descriptors
                 * means the connection is different than when the timeout
                 * was registered. Silently discard it.
                 */
                event_del(&t->ev);
                timeout_free_unsafe(t);
            }
        }
    }
    AZ(pthread_mutex_unlock(&timeout_list_mutex));
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

    AN(t);
    AN(t->c);
    if(!me || THREAD_EQL(me,t->c->thread))
        timeout_event_handler(fd,flags,arg);
    else
        timeout_check_idle(me);
}

/* NB: timeout_event_handler() runs in the timeout background thread */
static void timeout_event_handler(const int fd, const short flags, void *arg)
{
{
    timeout_event_t *t = (timeout_event_t*)arg;
    timeout_event_t *cause = NULL;
    conn *c;
    int fd;
    char buf[1] = {'T'};
    uint64_t next_sleep = (uint64_t)settings.timeout_thread_sleep;
    uint64_t idle_timeout = SEC_TO_USEC(settings.idle_timeout)
    uint64_t now = SEC_TO_USEC(current_time);
    struct timeval tv;
    AN(t);
    fd = t->fd;
    if (max_fds <= 0)
        max_fds = settings.maxconns;

    if(timeouts.lh_first != NULL) {
        timeout_event_t *ti, *next;
        /* scan the list of custom old timeouts to move to pending or destroy */
        AZ(pthread_mutex_lock(&timeout_list_mutex));
        for(ti = timeouts.lh_first; ti != NULL && (next = ti->link.le_next) != ti;
                                                                        ti = next)
        {
            uint64_t this_timeout;
            if(ti->fd == fd) {
                if(cause == NULL && fd > -1 && fd < max_fds && conns[fd] && ti->c) {
                    assert((c = ti->c) == conns[fd]);
                    this_timeout = TV_TO_USEC(&ti->tv);
                    if (CONN_CAN_TIMEOUT(c) &&
                        (now - SEC_TO_USEC(c->last_cmd_time)) >= this_timeout) {
                        uint64_t elapsed = now - SEC_TO_USEC(c->last_cmd_time);
                        if(elapsed <= idle_timeout && idle_timeout - elapsed < next_sleep)
                            next_sleep = idle_timeout - elapsed;
                        cause = ti;
                        event_del(&cause->ev);
                        /* move the timeout to the pending list and wake up
                         * the thread responsible.
                         */
                        /* reuse the event to wake up the connections's thread and close
                           the actual connection. We do this just once per connection
                           in case there were multiple timeouts scheduled.
                        */
                        AN(cause->c->thread && cause->c->thread->notify_send_fd > -1);
                        if(write(cause->c->thread->notify_send_fd,buf,1) == 1) {
                            LIST_REMOVE(cause,link);
                            LIST_INSERT_HEAD(&pending,cause,link);
                        } else {
                            perror("Failed writing timeout to notify pipe");
                            event_add(&cause->ev);
                        }
                        continue;
                    }
                }
                    /* An old timeout has been fully handled (fd == -1) or
                       the connection is invalid, either way discard it */
                    ti->fd = -1;
            }
            if(ti->fd == -1) {
                event_del(&ti->ev);
                timeout_free_unsafe(ti);
            }
        }
        AZ(pthread_mutex_unlock(&timeout_list_mutex));
    }

    if(cause == NULL) {
        int i;
        uint64_t elapsed;
        for (i = 0; i < max_fds; i++) {
            c = conns[i];
            if(!c || !CONN_CAN_TIMEOUT(c) || !c->thread)
                continue;
            if((elapsed = now - SEC_TO_USEC(c->last_cmd_time)) >= idle_timeout) {
                USEC_TO_TV(&tv,elapsed);
                cause = timeout_add_ex(c,&tv,0,&pending,
                                       NULL,timeout_close_handler);
                AN(cause);
                assert(c->thread && c->thread->notify_send_fd > -1);
                if (write(c->thread->notify_send_fd,buf,1) != 1) {
                    perror("Failed writing timeout to notify pipe");
                    USEC_TO_TV(&tv,settings.timeout_thread_sleep);
                    AZ(pthread_mutex_lock(&timeout_list_mutex));
                    cause->fd = c->fd;
                    LIST_REMOVE(cause,link);
                    LIST_INSERT_HEAD(&timeouts,cause,link);
                    AZ(pthread_mutex_unlock(&timeout_list_mutex));
                }
            } else if(elapsed < next_sleep)
                next_sleep = idle_timeout - elapsed;
        }
    }

    if(freelist_len >= MAX_FREE_TIMEOUTS) {
        AZ(pthread_mutex_lock(&timeout_list_mutex));
        for(t = freelist.lh_first; t != NULL && freelist_len > MAX_FREE_TIMEOUTS/2;
                                           freelist_len--, t = freelist.lh_first) {
            LIST_REMOVE(t,link);
            free(t);
        }
        AZ(pthread_mutex_unlock(&timeout_list_mutex));
    }

    if(t == &wakeup_timeout) {
        USEC_TO_TV(&t->tv,next_sleep);
        event_add(&t->ev,&t->tv);
    }
}

/****************************************************************************/
/* Timeout thread entry point                                               */
/****************************************************************************/
static void*
timeout_thread(void *c)
{
    AN(c);
    AZ(pthread_mutex_lock(&timeout_list_mutex));
    LIST_INIT(&timeouts);
    LIST_INIT(&freelist);
    LIST_INIT(&pending);
    AN(timeout_event_base = event_init());
    USEC_TO_TV(&wakeup_timeout.tv,settings.timeout_thread_sleep);
    wakeup_timeout.fd = -1;
    wakeup_timeout.c = NULL;
    event_set(&wakeup_timeout.ev,-1,0,timeout_event_handler,&wakeup_timeout);
    event_base_set(timeout_event_base,&wakeup_timeout.ev);
    AZ(pthread_cond_signal((pthread_cond_t*)c));
    AZ(pthread_mutex_unlock(&timeout_list_mutex));

    AZ(event_add(&wakeup_timeout.ev,&wakeup_timeout.tv));
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
    AZ(pthread_mutex_lock(&timeout_list_mutex));
    AZ(pthread_key_create(&current_thread_key,NULL));
    AZ(pthread_setspecific(current_thread_key,NULL));
    AZ(pthread_attr_init(&ta));
    AZ(pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED));
    AZ(pthread_create(&t,&ta,timeout_thread,&cond));
    while(timeout_event_base == NULL)
        AZ(pthread_cond_wait(&cond,&timeout_list_mutex));
    AZ(pthread_mutex_unlock(&timeout_list_mutex));
}

/* Allow a connection to register a timeout other than the default.
 *  when arg should be in microseconds. Returns -1 on failure
 */
int timeout_conn_set(conn *c, unsigned int when)
{
    struct timeval tv;
    timeout_event_t *t;

    if(!timeout_event_base)
        assert(pthread_once(&timeout_init_once,timeout_init));

    AN(c);
    AN(timeout_event_base);

    if(c->thread && pthread_setspecific(current_thread_key,c->thread) == -1)
        return -1;
    USEC_TO_TV(&tv,when);
    t = timeout_add_ex(c,&tv,EV_PERSIST,&pending,timeout_close_handler);
    return (t ? event_add(&t->ev) : -1);
}

/* Clear all timeouts for a given connection */
void timeout_conn_clear(conn *c)
{
    timeout_event_t *t, *next;

    if(!timeout_event_base)
        assert(pthread_once(&timeout_init_once,timeout_init));

    AN(timeout_event_base);

    if(c->thread)
        pthread_setspecific(current_thread_key,c->thread);

    AZ(pthread_mutex_lock(&timeout_list_mutex));
    for(t = pending.lh_first; t != NULL && (next = t->link.lh_next) == next; t = next) {
        if(t->c == c) {
            event_del(&t->ev);
            timeout_free_unsafe(t);
        }
    }
    for(t = timeouts.lh_first; t != NULL && (next = t->link.lh_next) == next; t = next) {
        if(t->c == c) {
            /* the timeout thread will clear these up later */
            t->fd = -1;
            t->c = NULL;
        }
    }
    AZ(pthread_mutex_unlock(&timeout_list_mutex));
}

/* Initialize maxfds and start timeout bg thread. *
 * This function can be called multiple times, once from the main thread
 * during initialization and once for each worker thread. When called from
 * worker thread initialization pass the thread id but max_conns as -1.
 * Passing thread id as NULL indicates the main thread.
 *//
void timeout_thread_init(int max_conns, LIBEVENT_THREAD *t)
{
    if(max_conns > max_fds)
        max_fds = max_conns;

    if(pthread_once(&timeout_init_once,timeout_init) == -1) {
        perror("pthread_once failed while initializing timeout thread");
        abort();
    }

    if(t != NULL)
        AZ(pthread_setspecific(current_thread_key,t);
}
