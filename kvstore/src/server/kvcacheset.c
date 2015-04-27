#include <pthread.h>
#include <errno.h>
#include <stdbool.h>
#include "uthash.h"
#include "utlist.h"
#include "kvconstants.h"
#include "kvcacheset.h"

/* Initializes CACHESET to hold a maximum of ELEM_PER_SET elements.
 * ELEM_PER_SET must be at least 2.
 * Returns 0 if successful, else a negative error code. */
int kvcacheset_init(kvcacheset_t *cacheset, unsigned int elem_per_set) {
  if (elem_per_set < 2)
    return -1;
  cacheset->elem_per_set = elem_per_set;
  int ret;
  if ((ret = pthread_rwlock_init(&cacheset->lock, NULL)) < 0)
    return ret;
  cacheset->num_entries = 0;
  cacheset->entries = NULL;
  return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  // OUR CODE HERE
  pthread_rwlock_rdlock(cacheset->lock);
  kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  e->value = (char*) malloc((strlen(*value)+1)*sizeof(char));
  strcpy(e->value, *value);
  pthread_rwlock_unlock(cacheset->lock);
  if (e == NULL) {
    return ERRNOKEY;
  }
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  // OUR CODE HERE
  pthread_rwlock_wrlock(cacheset->lock);
  
  if (cacheset->num_entries < cacheset->elem_per_set) {
    cacheset->num_entries++;
  } else {
    // evict element
  }

  kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    e = (struct kvcacheentry *) malloc(sizeof(struct kvcacheentry));
    e->key = key;
    // e->refcnt = 0;
    HASH_ADD_STR(cacheset->entries, key, e);
  }
  // do we need to malloc?
  strcpy(e->value, *value);
  
  pthread_rwlock_unlock(cacheset->lock);
  return 0;
  // return -1;  
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  // OUR CODE HERE
  if (cacheset->num_entries == 0) {
    return -1;
  } 
  pthread_rwlock_wrlock(cacheset->lock);
  cacheset->num_entries--;
  kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    return ERRNOKEY;
  }
  HASH_DEL(cacheset->entries, e);
  free(e);
  pthread_rwlock_unlock(cacheset->lock);
  }

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
  HASH_CLEAR(hh, cacheset->entries);
}
