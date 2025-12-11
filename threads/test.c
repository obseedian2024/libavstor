/*
* Simple program to test threading
*
* BSD 3-Clause License
*
* Copyright (c) 2025, Tamas Fejerpataky
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* 1. Redistributions of source code must retain the above copyright notice, this
*    list of conditions and the following disclaimer.
*
* 2. Redistributions in binary form must reproduce the above copyright notice,
*    this list of conditions and the following disclaimer in the documentation
*    and/or other materials provided with the distribution.
*
* 3. Neither the name of the copyright holder nor the names of its
*    contributors may be used to endorse or promote products derived from
*    this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
* FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
* SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
* CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
* OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "stdatomic.h"
#include "threads.h"

#define iterations 100000
#define total_prod 4
#define total_cons 4
#define QUEUE_SIZE 512

typedef struct consumer {
    thrd_t thr;
    unsigned long consumed;
    __int64 total;
} consumer_t;

typedef struct producer {
    thrd_t thr;
    unsigned long produced;
    long N1, N2;
} producer_t;

struct queue
{
    size_t len;
    size_t count;
    long *head;
    long *tail;
    long *last;
    long buf[];
};

struct queue *queue_create(const size_t length)
{
    struct queue *queue = malloc(
        sizeof(struct queue) + length * sizeof(long));

    if (queue == NULL) return queue;

    queue->len = length;

    queue->head = queue->tail = &queue->buf[0];
    queue->last = &queue->buf[queue->len - 1];
    queue->count = 0;
    return queue;
}

void queue_free(struct queue *queue)
{
    free(queue);
}

int queue_tryenqueue(struct queue *queue, long item)
{
    int res = 0;
    if (queue->count < queue->len) {
        res = 1;
        *queue->tail = item;
        if (++queue->tail > queue->last) {
            queue->tail = &queue->buf[0];
        }
        queue->count++;
    }
    return res;
}

int queue_trydequeue(struct queue *queue, long *item)
{
    int res = 0;
    if (queue->count > 0) {
        res = 1;
        *item = *queue->head;
        if (++queue->head > queue->last) {
            queue->head = &queue->buf[0];
        }
        queue->count--;
    }
    return res;
}

int queue_getcount(struct queue *queue)
{
    return queue->count;
}

mtx_t _mtx_queue;
mtx_t _mtx_done;

cnd_t _cnd_not_empty;
cnd_t _cnd_not_full;
cnd_t _cnd_done;

int die = 0;
int done = 0;

struct queue *qu;

static int __cdecl prod_func(void *param)
{
    producer_t *p = (producer_t*)param;
    long i;
    p->produced = 0;
    for (i = p->N1; i <= p->N2; i++)
    {
        mtx_lock(&_mtx_queue);
        while (!queue_tryenqueue(qu, i)) {
            cnd_wait(&_cnd_not_full, &_mtx_queue);
        }
        cnd_signal(&_cnd_not_empty);
        mtx_unlock(&_mtx_queue);
        p->produced++;
    }
    mtx_lock(&_mtx_done);
    done--;
    if (done == 0) {
        cnd_broadcast(&_cnd_done);
    }
    mtx_unlock(&_mtx_done);
    return 0;
}

static int __cdecl cons_func(void *param)
{
    consumer_t *p = (consumer_t *)param;
    mtx_lock(&_mtx_queue);
    while (1) {
        long item = 0;
        if (queue_trydequeue(qu, &item)) {
            cnd_signal(&_cnd_not_full);
            mtx_unlock(&_mtx_queue);

            p->consumed++;
            p->total += (__int64)item;
            mtx_lock(&_mtx_queue);
        }
        else if (die) {
            mtx_unlock(&_mtx_queue);
            break;
        }
        else {
            cnd_wait(&_cnd_not_empty, &_mtx_queue);
        }
    }
    return 0;
}

consumer_t cons[total_cons];
producer_t prod[total_prod];
thrd_t threads[total_prod + total_cons];

int main(void)
{
    clock_t start_time, end_time;
    const long num = iterations / total_prod;
    __int64 total = 0;
    int i;
    double diff;
    tss_t dummy;

    tss_create(&dummy, NULL);
    qu = queue_create(QUEUE_SIZE);

    mtx_init(&_mtx_queue, mtx_plain);
    mtx_init(&_mtx_done, mtx_plain);

    cnd_init(&_cnd_not_empty);
    cnd_init(&_cnd_not_full);
    cnd_init(&_cnd_done);

    done = total_prod;
    die = 0;

    start_time = clock();

    for (i = 0; i < total_cons; i++) {
        cons[i].consumed = 0;
        cons[i].total = 0;
        thrd_create(&cons[i].thr, &cons_func, &cons[i]);
    }

    for (i = 0; i < total_prod; i++)
    {
        prod[i].N1 = 1 + num * i;
        prod[i].N2 = num * (i + 1);
        thrd_create(&prod[i].thr, &prod_func, &prod[i]);
    }

    mtx_lock(&_mtx_done);
    while (done > 0) {
        cnd_wait(&_cnd_done, &_mtx_done);
    }
    mtx_unlock(&_mtx_done);

    mtx_lock(&_mtx_queue);
    die = 1;
    cnd_broadcast(&_cnd_not_empty);
    mtx_unlock(&_mtx_queue);

    for (i = 0; i < total_prod; i++)
    {
        int res;
        thrd_join(prod[i].thr, &res);

        printf("Producer %i produced %li items\n", i, prod[i].produced);
    }

    for (i = 0; i < total_cons; i++)
    {
        int res;
        thrd_join(cons[i].thr, &res);
        total += cons[i].total;
        printf("Consumer %i consumed %li items\n", i, cons[i].consumed);
    }

    end_time = clock();
    diff = (double)(end_time - start_time) / CLOCKS_PER_SEC;
    printf("Sum = %lli\n", total);
    printf("Elapsed time: %f\n", diff);
    printf("Iterations per ms: %4.8G\n", (double)iterations / diff / 1000.0);
    return 0;
}
