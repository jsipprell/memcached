/* High-water marker for number of timeout structures in freelist before 50% of them
 * are freed.
 */
#ifndef MAX_FREE_TIMEOUTS
#define MAX_FREE_TIMEOUTS 100
#endif

/* Called by each thread in thread.c when notified of possible idle conns */
extern void mc_timeout_check_idle(LIBEVENT_THREAD*);
/* Allows a connection to set a custom timeout less than the default */
extern int mc_timeout_set(conn*,unsigned int when);
/* Clear all specific timeouts for a connection */
extern void mc_timeout_clear(conn*);
/* Initialize the background timeout thread, can be called both from the main
 * thread during startup (pass thread id NULL) and from each worker thread.
 * max_conns can only increase, passing -1 will never change the max fds.
 */
extern void mc_timeout_init(int max_conns, LIBEVENT_THREAD *id);

/* from memcached.c */
extern void conn_timeout(conn*, struct timeval *timeout, uint64_t elapsed);

/* Instead of sys/queue.h, we have .... */
#if NEED_LL_DECL == 1
#define LL_LINK(type) type##_ptr next
#define LL_DECL(type) \
typedef struct type *type##_ptr; \
static inline type##_ptr ll_r_##type(type##_ptr *le, type##_ptr e) \
{ \
    type##_ptr oe = *le; \
    *le = e; \
    return oe; \
} \
struct type##_list { struct type *first; }; \
typedef struct type

#define LL_INIT(head) ((head)->first = NULL)
#define LL_EMPTY(head) ((head) == NULL || (head)->first == NULL)
#define LL_FIRST(head) (&((head)->first))
#define LL_NEXT(ep) (*(ep) ? &((*(ep))->next) : NULL)
#define LL_INSERT(head,e) do { \
    if((e) != NULL) (e)->next = (head)->first; \
    (head)->first = (e); \
} while(0)

#define LL_REMOVE_ELEM(ep,type) ll_r_##type((ep),(*(ep) ? (*(ep))->next : NULL))
#define LL_FOREACH(head,ep) \
        for((ep) = LL_FIRST(head); (ep) != NULL && (*(ep) != NULL); (ep) = LL_NEXT(ep))
#define LL_FOREACH_E(head,ep,e) \
        for ((ep) = LL_FIRST(head), (e) = NULL; (ep) != NULL && (((e) = *(ep)) != NULL); \
                                                                    (ep) = LL_NEXT(ep))
#define LL_FINDP(head,ep,e) do { \
        LL_FOREACH(head,ep) \
            if(*(ep) == (e)) break; \
        if(!*(ep)) (ep) = NULL; \
} while(0)
#endif /* NEED_LL_DECL */

