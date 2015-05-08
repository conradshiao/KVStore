#ifndef __KV_CACHE_SET__
#define __KV_CACHE_SET__

#include <pthread.h>
#include <stdbool.h>
#include "uthash.h"
// OUR CODE HERE
#include "utlist.h"

/* KVCacheSet represents a single distinct set of elements within a KVCache.
 *
 * Elements within a KVCacheSet may not be accessed/modified concurrently. The
 * read-write lock within the KVCacheSet struct should be used to enforce this.
 * The lock should be acquired/released from whoever will be calling the 
 * cache methods (i.e. KVServer and, later, TPCMaster).
 *
 * A KVCacheSet may not store more than ELEM_PER_SET entries. The eviction
 * policy used is the second-chance algorithm. See kvcache.h for more details
 * on this algorithm.
 */

/* An entry within the KVCacheSet. */
struct kvcacheentry {
  char *key;                    /* The entry's key. */
  char *value;                  /* The entry's value. */
  bool refbit;                  /* Used to determine if this entry has been used. */

  // OUR CODE HERE
  UT_hash_handle hh;            /* Handle to allow ut_hash operations for the
                                   kvcacheentry hashtable in struct kvcacheset_t. */

  /* These pointers are needed to implement a kvcacheset_t's doubly-linked list. */
  struct kvcacheentry *prev;
  struct kvcacheentry *next;
};

/* A KVCacheSet. */
typedef struct {
  unsigned int elem_per_set;      /* The max number of elements which can be stored in this set. */
  pthread_rwlock_t lock;          /* The lock which can be used to lock this set. */
  int num_entries;                /* The current number of entries in this set. */
  
  // OUR CODE HERE
  struct kvcacheentry *head;	    /* List view of my kvcacheentries. */
  struct kvcacheentry *entries;   /* Hash table view of my kvcacheentries. */
} kvcacheset_t;

int kvcacheset_init(kvcacheset_t *, unsigned int elem_per_set);

int kvcacheset_get(kvcacheset_t *, char *key, char **value);
int kvcacheset_put(kvcacheset_t *, char *key, char *value);
int kvcacheset_del(kvcacheset_t *, char *key);

void kvcacheset_clear(kvcacheset_t *);

#endif
