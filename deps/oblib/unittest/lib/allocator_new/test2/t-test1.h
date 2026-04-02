/*
 * $Id: t-test1.c,v 1.2 2006/03/27 16:05:13 wg Exp $
 * by Wolfram Gloger 1996-1999, 2001, 2004, 2006
 * A multi-thread test for malloc performance, maintaining one pool of
 * allocated bins per thread.
 */

#if (defined __STDC__ && __STDC__) || defined __cplusplus
# include <stdlib.h>
#endif
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/mman.h>

#include "lran2.h"
#include "t-test.h"


#include "thread-st.h"

#include "lib/alloc/ob_malloc_allocator.h"

struct bin_info {
	struct bin *m;
	unsigned long size, bins;
};

#if ALLOCATOR_TEST > 0

void
bin_test1(struct bin_info *p)
{
	int b;

	for(b=0; b<p->bins; b++) {
		if(mem_check(p->m[b].ptr, p->m[b].size)) {
			printf("memory corrupt!\n");
			abort();
		}
	}
}

#endif

template<class Allocator>
void malloc_test1(struct thread_st *st)
{
	BinAllocator<Allocator> &allocator = *(BinAllocator<Allocator>*)st->allocator;
	int b, i, j, actions, pid = 1;
	struct bin_info p;
	struct lran2_st ld; /* data for random number generator */

	lran2_init(&ld, st->u.seed);
#if TEST_FORK>0
	if(RANDOM(&ld, TEST_FORK) == 0) {
		int status;

#if !USE_THR
		pid = fork();
#else
		pid = fork1();
#endif
		if(pid > 0) {
		    /*printf("forked, waiting for %d...\n", pid);*/
			waitpid(pid, &status, 0);
			printf("done with %d...\n", pid);
			if(!WIFEXITED(status)) {
				printf("child term with signal %d\n", WTERMSIG(status));
				exit(1);
			}
			return;
		}
		exit(0);
	}
#endif
	p.m = (struct bin *)std::malloc(st->u.bins*sizeof(*p.m));
	p.bins = st->u.bins;
	p.size = st->u.size;
	for(b=0; b<p.bins; b++) {
		p.m[b].size = 0;
		p.m[b].ptr = NULL;
		if(RANDOM(&ld, 2) == 0)
			allocator.bin_alloc(&p.m[b], RANDOM(&ld, p.size) + 1, lran2(&ld));
	}
	for(i=0; i<=st->u.max;) {
#if ALLOCATOR_TEST > 1
		bin_test1(&p);
#endif
		actions = RANDOM(&ld, ACTIONS_MAX);
#if USE_MALLOC && MALLOC_DEBUG
		if(actions < 2) { mallinfo(); }
#endif
		for(j=0; j<actions; j++) {
			b = RANDOM(&ld, p.bins);
			allocator.bin_free(&p.m[b]);
		}
		i += actions;
		actions = RANDOM(&ld, ACTIONS_MAX);
		for(j=0; j<actions; j++) {
			b = RANDOM(&ld, p.bins);
			allocator.bin_alloc(&p.m[b], RANDOM(&ld, p.size) + 1, lran2(&ld));
#if ALLOCATOR_TEST > 2
			bin_test1(&p);
#endif
		}
#if 0 /* Test illegal free()s while setting MALLOC_CHECK_ */
		for(j=0; j<8; j++) {
			b = RANDOM(&ld, p.bins);
			if(p.m[b].ptr) {
			  int offset = (RANDOM(&ld, 11) - 5)*8;
			  char *rogue = (char*)(p.m[b].ptr) + offset;
			  /*printf("p=%p rogue=%p\n", p.m[b].ptr, rogue);*/
			  free(rogue);
			}
		}
#endif
		i += actions;
	}
	for(b=0; b<p.bins; b++)
		allocator.bin_free(&p.m[b]);
	std::free(p.m);
	if(pid == 0)
		exit(0);
}

#if 0
/* Protect address space for allocation of n threads by LinuxThreads.  */
static void
protect_stack(int n)
{
	char buf[2048*1024];
	char* guard;
	size_t guard_size = 2*2048*1024UL*(n+2);

	buf[0] = '\0';
	guard = (char*)(((unsigned long)buf - 4096)& ~4095UL) - guard_size;
	printf("Setting up stack guard at %p\n", guard);
	if(mmap(guard, guard_size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED,
			-1, 0)
	   != guard)
		printf("failed!\n");
}
#endif

template<class Allocator, class... Args>
int run_test1(int n_total_max, int n_thr, int64_t i_max, int64_t size, const Args&... args)
{

	BinAllocator<Allocator> allocator;
	allocator.init(args...);
	struct thread_st *st;
	int bins = MEMORY/(size*n_thr);
	/*protect_stack(n_thr);*/

	thread_init();
	printf("total=%d threads=%d i_max=%ld size=%ld bins=%d\n",
		   n_total_max, n_thr, i_max, size, bins);

	st = (struct thread_st *)std::malloc(n_thr*sizeof(*st));
	if(!st) exit(-1);

	/* Start all n_thr threads. */
	for(int i=0; i<n_thr; i++) {
		st[i].u.bins = bins;
		st[i].u.max = i_max;
		st[i].u.size = size;
		st[i].u.seed = ((long)i_max*size + i) ^ bins;
		st[i].sp = 0;
		st[i].func = malloc_test1<Allocator>;
		st[i].allocator = &allocator;
		if(thread_create(&st[i])) {
			printf("Creating thread #%d failed.\n", i);
			n_thr = i;
			break;
		}
		printf("Created thread %lx.\n", (long)st[i].id);
	}
	int n_running = n_thr;
	int n_total = n_thr;
	while (n_running>0) {
		wait_for_thread(st, n_thr, my_end_thread, n_running, n_total, n_total_max);
	}
	for(int i=0; i<n_thr; i++) {
		std::free(st[i].sp);
	}
	std::free(st);
	printf("Done.\n");
	return 0;
}


/*
 * Local variables:
 * tab-width: 4
 * End:
 */
