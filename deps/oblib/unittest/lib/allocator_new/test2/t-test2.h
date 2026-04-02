/*
 * $Id: t-test2.c,v 1.2 2006/03/27 16:06:20 wg Exp $
 * by Wolfram Gloger 1996-1999, 2001, 2004
 * A multi-thread test for malloc performance, maintaining a single
 * global pool of allocated bins.
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

#include "lran2.h"
#include "t-test.h"

#include "thread-st.h"
#include "malloc-machine.h" /* for mutex */

#include "lib/alloc/ob_malloc_allocator.h"

struct block {
	struct bin b[BINS_PER_BLOCK];
	mutex_t mutex;
} *blocks;

int n_blocks;

#if ALLOCATOR_TEST > 0

void
bin_test2(void)
{
	int b, i;

	for(b=0; b<n_blocks; b++) {
		test_mutex_lock(&blocks[b].mutex);
		for(i=0; i<BINS_PER_BLOCK; i++) {
			if(mem_check(blocks[b].b[i].ptr, blocks[b].b[i].size)) {
				printf("memory corrupt!\n");
				exit(1);
			}
		}
		test_mutex_unlock(&blocks[b].mutex);
	}
}

#endif
template<class Allocator>
void malloc_test2(struct thread_st *st)
{
	BinAllocator<Allocator> &allocator = *(BinAllocator<Allocator>*)st->allocator;
	struct block *bl;
	int i, b, r;
	struct lran2_st ld; /* data for random number generator */
	unsigned long rsize[BINS_PER_BLOCK];
	int rnum[BINS_PER_BLOCK];

	lran2_init(&ld, st->u.seed);
	for(i=0; i<=st->u.max;) {
#if ALLOCATOR_TEST > 1
		bin_test2();
#endif
		bl = &blocks[RANDOM(&ld, n_blocks)];
		r = RANDOM(&ld, 1024);
		if(r < 200) { /* free only */
			test_mutex_lock(&bl->mutex);
			for(b=0; b<BINS_PER_BLOCK; b++)
				allocator.bin_free(&bl->b[b]);
			test_mutex_unlock(&bl->mutex);
			i += BINS_PER_BLOCK;
		} else { /* alloc/realloc */
			/* Generate random numbers in advance. */
			for(b=0; b<BINS_PER_BLOCK; b++) {
				rsize[b] = RANDOM(&ld, st->u.size) + 1;
				rnum[b] = lran2(&ld);
			}
			test_mutex_lock(&bl->mutex);
			for(b=0; b<BINS_PER_BLOCK; b++)
				allocator.bin_alloc(&bl->b[b], rsize[b], rnum[b]);
			test_mutex_unlock(&bl->mutex);
			i += BINS_PER_BLOCK;
		}
#if ALLOCATOR_TEST > 2
		bin_test2();
#endif
	}
}

template<class Allocator, class... Args>
int run_test2(int n_total_max, int n_thr, int64_t i_max, int64_t size, const Args&... args)
{
	BinAllocator<Allocator> allocator;
	allocator.init(args...);
	struct thread_st *st;
	int bins = MEMORY/size;
	if(bins < BINS_PER_BLOCK) bins = BINS_PER_BLOCK;

	n_blocks = bins/BINS_PER_BLOCK;
	blocks = (struct block *)std::malloc(n_blocks*sizeof(*blocks));
	if(!blocks)
		exit(1);

	thread_init();
	printf("total=%d threads=%d i_max=%ld size=%ld bins=%d\n",
		   n_total_max, n_thr, i_max, size, n_blocks*BINS_PER_BLOCK);

	for(int i=0; i<n_blocks; i++) {
		test_mutex_init(&blocks[i].mutex);
		for(int j=0; j<BINS_PER_BLOCK; j++) blocks[i].b[j].size = 0;
	}

	st = (struct thread_st *)std::malloc(n_thr*sizeof(*st));
	if(!st) exit(-1);
	/* Start all n_thr threads. */
	for(int i=0; i<n_thr; i++) {
		st[i].u.max = i_max;
		st[i].u.size = size;
		st[i].u.seed = ((long)i_max*size + i) ^ n_blocks;
		st[i].sp = 0;
		st[i].func = malloc_test2<Allocator>;
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

	for(int i=0; i<n_blocks; i++) {
		for(int j=0; j<BINS_PER_BLOCK; j++)
			allocator.bin_free(&blocks[i].b[j]);
	}

	for(int i=0; i<n_thr; i++) {
		std::free(st[i].sp);
	}
	std::free(st);
	std::free(blocks);
	printf("Done.\n");
	return 0;
}

/*
 * Local variables:
 * tab-width: 4
 * End:
 */
