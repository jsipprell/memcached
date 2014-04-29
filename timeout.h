/* Called by each thread in thread.c when notified of possible idle conns */
void timeout_check_idle(LIBEVENT_THREAD*);
/* Allows a connection to set a custom timeout less than the default */
int timeout_conn_set(conn*,unsigned int when);
/* Clear all specific timeouts for a connection */
void timeout_conn_clear(conn*);
/* Initialize the background timeout thread, can be called both from the main
 * thread during startup (pass thread id NULL) and from each worker thread.
 * max_conns can only increase, passing -1 will never change the max fds.
 */
void timeout_thread_init(int max_conns, LIBEVENT_THREAD *id);
