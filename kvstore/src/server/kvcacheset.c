#include <pthread.h>
#include <errno.h>
#include <stdbool.h>
#include "uthash.h"
#include "utlist.h"
#include "kvconstants.h"
#include "kvcacheset.h"
// OUR CODE HERE
#include <stdlib.h>
#include <string.h>

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
  return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  // OUR CODE HERE
  struct kvcacheentry *e;
 
  pthread_rwlock_rdlock(&cacheset->lock);
  HASH_FIND_STR(cacheset->entries, key, e);
  pthread_rwlock_unlock(&cacheset->lock);

  if (e == NULL) {
    return ERRNOKEY;
  }

  *value = (char *) malloc((strlen(e->value) + 1) * sizeof(char));
  strcpy(*value, e->value); 
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  // OUR CODE HERE
  pthread_rwlock_rdlock(&cacheset->lock);
  
  if (cacheset->num_entries < cacheset->elem_per_set) {
    cacheset->num_entries++;
  } else {
    //FIXME
    // evict element: use 2nd change algorithm here
  }

  struct kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    e = (struct kvcacheentry *) malloc(sizeof(struct kvcacheentry));
    if (e == NULL) {
      pthread_rwlock_unlock(&cacheset->lock);
      // some type of error: running out of memory?
      return -1; // temporary error
    }
    strcpy(e->key, (const char *) key);
    HASH_ADD_STR(cacheset->entries, key, e);
    // do we need to check if above line was successful? and if it wasn't to return an error code?
  }
  e->refbit = true; // do i need to put this in the if loop? atm, it refreshes for every put operation
  strcpy(e->value, (const char *) value);

  pthread_rwlock_unlock(&cacheset->lock);
  return 0;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  // OUR CODE HERE
  // 1. We might not need this if hash_find_str takes care of this for us on empty hashtables
  // 2. Should we support synchronization here and wait until something is deleted? i srsly doubt this option
  if (cacheset->num_entries == 0) {
    return -1;
  }

  struct kvcacheentry *e;
  pthread_rwlock_wrlock(&cacheset->lock);
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    return ERRNOKEY;
  }
  HASH_DEL(cacheset->entries, e); // see if this was successful? If we need to, then check if not successful and exit on error
  free(e);
  cacheset->num_entries--;
  pthread_rwlock_unlock(&cacheset->lock);
  return 0;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
  HASH_CLEAR(hh, cacheset->entries);
  // README: do we need to destroy this lock or not? when is the cacheset freed? and even when it is, do we need to destroy the lock or does free-ing take care of it?
  // pthread_rwlock_destroy(&cacheset->lock);
}
