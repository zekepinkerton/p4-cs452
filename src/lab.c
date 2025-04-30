#include "lab.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>


struct queue {
    void **buffer;               // Circular buffer to store elements
    int capacity;                // Maximum number of elements in the queue
    int size;                    // Current number of elements
    int front;                   // Index of the front element
    int rear;                    // Index of the last element
    bool is_shutdown_flag;      // Indicates whether the queue is shut down
    pthread_mutex_t lock;       // Mutex to protect shared resources
    pthread_cond_t not_full;    // Condition variable: queue not full
    pthread_cond_t not_empty;   // Condition variable: queue not empty
};


queue_t queue_create(int capacity) {
    assert(capacity > 0);

    queue_t q = (queue_t)malloc(sizeof(struct queue));
    if (q == NULL) {
        perror("Failed to allocate memory for queue");
        return NULL;
    }

    q->buffer = (void **)malloc(capacity * sizeof(void *));
    if (q->buffer == NULL) {
        perror("Failed to allocate memory for queue buffer");
        free(q);
        return NULL;
    }

    q->capacity = capacity;
    q->size = 0;
    q->front = 0;
    q->rear = -1;
    q->is_shutdown_flag = false;

    if (pthread_mutex_init(&q->lock, NULL) != 0) {
        perror("Failed to initialize mutex");
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        perror("Failed to initialize not_full condition");
        pthread_mutex_destroy(&q->lock);
        free(q->buffer);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        perror("Failed to initialize not_empty condition");
        pthread_cond_destroy(&q->not_full);
        pthread_mutex_destroy(&q->lock);
        free(q->buffer);
        free(q);
        return NULL;
    }

    return q;
}


void queue_destroy(queue_t q) {
    if (q == NULL) {
        return;
    }

    // Signal shutdown and wake all waiting threads
    queue_initiate_shutdown(q);

    pthread_mutex_lock(&q->lock);
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
    pthread_mutex_destroy(&q->lock);

    free(q->buffer);
    free(q);
}


void queue_push(queue_t q, void *data) {
    if (q == NULL) {
        return;
    }

    pthread_mutex_lock(&q->lock);

    // Wait while the queue is full and not shutting down
    while (q->size == q->capacity && !q->is_shutdown_flag) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    if (q->is_shutdown_flag) {
        pthread_mutex_unlock(&q->lock);
        return;
    }

    // Insert data into the circular buffer
    q->rear = (q->rear + 1) % q->capacity;
    q->buffer[q->rear] = data;
    q->size++;

    // Notify consumers that data is available
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}


void *queue_pop(queue_t q) {
    if (q == NULL) {
        return NULL;
    }

    pthread_mutex_lock(&q->lock);

    // Wait while the queue is empty and not shutting down
    while (q->size == 0 && !q->is_shutdown_flag) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    if (q->size == 0) {
        pthread_mutex_unlock(&q->lock);
        return NULL;
    }

    // Remove and return the front element
    void *data = q->buffer[q->front];
    q->front = (q->front + 1) % q->capacity;
    q->size--;

    // Notify producers that space is available
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);

    return data;
}


void queue_initiate_shutdown(queue_t q) {
    if (q == NULL) {
        return;
    }

    pthread_mutex_lock(&q->lock);
    q->is_shutdown_flag = true;

    // Wake all producers and consumers
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}


bool queue_is_empty(queue_t q) {
    if (q == NULL) {
        return true;
    }

    pthread_mutex_lock(&q->lock);
    bool empty = (q->size == 0);
    pthread_mutex_unlock(&q->lock);

    return empty;
}


bool queue_is_shutdown(queue_t q) {
    if (q == NULL) {
        return true;
    }

    pthread_mutex_lock(&q->lock);
    bool shutdown = q->is_shutdown_flag;
    pthread_mutex_unlock(&q->lock);

    return shutdown;
}
