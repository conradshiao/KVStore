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

static void kvcacheentry_free(kvcacheentry *e)

/* Initializes CACHESET to hold a maximum of ELEM_PER_SET elements.
 * ELEM_PER_SET must be at least 2.
 * Returns 0 if successful, else a negative error code. */
int kvcacheset_init(kvcacheset_t *cacheset, unsigned int elem_per_set) {
  if (elem_per_set < 2)
    return -1;
  int ret;
  if ((ret = pthread_rwlock_init(&cacheset->lock, NULL)) < 0)
    return ret;
  cacheset->elem_per_set = elem_per_set;
  cacheset->num_entries = 0;
  // OUR CODE HERE
  cacheset->entries = NULL;
  cacheset->head = NULL;
  return 0;
}


/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  // OUR CODE HERE
  struct kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    return ERRNOKEY;
  }
  *value = (char *) malloc((strlen(e->value) + 1) * sizeof(char));
  if (*value == NULL) {
    return -1;
  }
  e->refbit = true;
  strcpy(*value, e->value);
  return 0;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  // OUR CODE HERE
  struct kvcacheentry *e;

  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) { // cacheset does NOT contain this key already
    if (cacheset->num_entries < cacheset->elem_per_set) {
      cacheset->num_entries++;
    } else { // evicting an element: use 2nd change algorithm here
      while (true) {
        struct kvcacheentry *candidate = cacheset->head;
        DL_DELETE(cacheset->head, candidate);
        if (candidate->refbit) {
          candidate->refbit = false;
          DL_APPEND(cacheset->head, candidate);
        } else {
          HASH_DEL(cacheset->entries, candidate);
          kvcacheentry_free(candidate);
          break; 
        }
      }
    }
    e = (struct kvcacheentry *) malloc(sizeof(struct kvcacheentry));
    if (e == NULL) {
      return -1;
    }
    e->key = (char *) malloc((strlen(key) + 1) * sizeof(char));
    if (e->key == NULL) {
      kvcacheentry_free(e);
      return -1;
    }
    strcpy(e->key, key);
    HASH_ADD_STR(cacheset->entries, key, e);
    DL_APPEND(cacheset->head, e);
    e->refbit = false;
  } else {
    free(e->value); // the value is about to be overwritten, so free old value
    e->refbit = true;
  }

  e->value = (char *) malloc((strlen(value) + 1) * sizeof(char));
  if (e->value == NULL) {
    kvcacheentry_free(e);
    return -1;
  }
  strcpy(e->value, value);

  return 0;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  // OUR CODE HERE
  struct kvcacheentry *e;
  HASH_FIND_STR(cacheset->entries, key, e);
  if (e == NULL) {
    return ERRNOKEY;
  }
  HASH_DEL(cacheset->entries, e);
  DL_DELETE(cacheset->head, e);
  kvcacheentry_free(e);
  cacheset->num_entries--;
  return 0;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
  // OUR CODE HERE
  HASH_CLEAR(hh, cacheset->entries);
  struct kvcacheentry *elt, *tmp;
  DL_FOREACH_SAFE(cacheset->head, elt, tmp) {
    DL_DELETE(cacheset->head, elt);
    kvcacheentry_free(elt);
  }
}

// OUR CODE HERE
/* Frees the kvcacheentry element and all of it's malloc()-ed fields. */
static void kvcacheentry_free(kvcacheentry *e) {
  if (e != NULL) {
    free(e->key);
    free(e->value);
    free(e);
  }
}
