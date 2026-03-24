/*
 * $Id: thread-st.h$
 * pthread version
 * by Wolfram Gloger 2004
 */

#ifndef THREAD_ST_H
#define THREAD_ST_H
#include <pthread.h>
#include <stdio.h>

pthread_cond_t finish_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t finish_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifndef USE_PTHREADS_STACKS
#define USE_PTHREADS_STACKS 0
#endif

#ifndef STACKSIZE
#define STACKSIZE	32768
#endif

struct user_data {
	int bins, max;
	unsigned long size;
	long seed;
};

struct thread_st {
	char *sp;							/* stack pointer, can be 0 */
	void (*func)(struct thread_st* st);	/* must be set by user */
	pthread_t id;
	int flags;
	struct user_data u;
	void* allocator;
};

static void
thread_init(void)
{
	printf("Using posix threads.\n");
	pthread_cond_init(&finish_cond, NULL);
	pthread_mutex_init(&finish_mutex, NULL);
}

static void *
thread_wrapper(void *ptr)
{
	struct thread_st *st = (struct thread_st*)ptr;

	/*printf("begin %p\n", st->sp);*/
	st->func(st);
	pthread_mutex_lock(&finish_mutex);
	st->flags = 1;
	pthread_mutex_unlock(&finish_mutex);
	pthread_cond_signal(&finish_cond);
	/*printf("end %p\n", st->sp);*/
	return NULL;
}

/* Create a thread. */
static int
thread_create(struct thread_st *st)
{
	st->flags = 0;
	{
		pthread_attr_t* attr_p = 0;
#if USE_PTHREADS_STACKS
		pthread_attr_t attr;

		pthread_attr_init (&attr);
		if(!st->sp)
			st->sp = malloc(STACKSIZE+16);
		if(!st->sp)
			return -1;
		if(pthread_attr_setstacksize(&attr, STACKSIZE))
			fprintf(stderr, "error setting stacksize");
		else
			pthread_attr_setstackaddr(&attr, st->sp + STACKSIZE);
		/*printf("create %p\n", st->sp);*/
		attr_p = &attr;
#endif
		return pthread_create(&st->id, attr_p, thread_wrapper, st);
	}
	return 0;
}

/* Wait for one of several subthreads to finish. */
static void
wait_for_thread(struct thread_st st[], int n_thr,
				int (*end_thr)(struct thread_st*, int&, int&, int&),
				int& n_running, int& n_total, int& n_total_max)
{
	int i;

	pthread_mutex_lock(&finish_mutex);
	for(;;) {
		int term = 0;
		for(i=0; i<n_thr; i++)
			if(st[i].flags) {
				/*printf("joining %p\n", st[i].sp);*/
				if(pthread_join(st[i].id, NULL) == 0) {
					st[i].flags = 0;
					if(end_thr)
						end_thr(&st[i], n_running, n_total, n_total_max);
				} else
					fprintf(stderr, "can't join\n");
				++term;
			}
		if(term > 0)
			break;
		pthread_cond_wait(&finish_cond, &finish_mutex);
	}
	pthread_mutex_unlock(&finish_mutex);
}

int my_end_thread(struct thread_st *st, int& n_running, int& n_total, int& n_total_max)
{
	/* Thread st has finished.  Start a new one. */
#if 0
	printf("Thread %lx terminated.\n", (long)st->id);
#endif
	if(n_total >= n_total_max) {
		n_running--;
	} else if(st->u.seed++, thread_create(st)) {
		printf("Creating thread #%d failed.\n", n_total);
	} else {
		n_total++;
		if(n_total%50 == 0)
			printf("n_total = %d\n", n_total);
	}
	return 0;
}

#endif
/*
 * Local variables:
 * tab-width: 4
 * End:
 */
