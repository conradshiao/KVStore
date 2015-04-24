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
  return 0;
}

// OUR CODE HERE: not rlly. just a comment. kvcacheentry is not used anywhere in code, so
// obviously we gotta change up the kcvacheset_t data structure to take in either an array
// or pointers to kvcachentries. 

/* Get the entry corresponding to KEY from CACHESET. Returns 0 if successful,
 * else returns a negative error code. If successful, populates VALUE with a
 * malloced string which should later be freed. */
int kvcacheset_get(kvcacheset_t *cacheset, char *key, char **value) {
  // OUR CODE HERE
  long index = hash(key);
  if (index >= cacheset->num_entries) {
    return -1; // i need to look up what type of error this rlly is in kvconstants.h
  }
  // does the comment imply that i need to malloc? and if so.. where do we free? And do they already do that for us?
  pthread_rwlock_rdlock(cacheset->lock);
  /* I think this is whats happening.
  Value is char ** because C is pass by copy, so we rlly care about *value (what value points to)
  We malloc *value (this part is iffy), then copy the corresponding value found in the
  cache into this malloced value, and return? Idk if i'm right*/
  char *wanted_value = cacheset->entries[index]->value;
  // do we malloc *value or value itself? uh. idk. i think maybe 2nd option?
  value = (char **) malloc(sizeof(char **)); // uh. man. i rlly don't know which one we malloc
  value = wanted_value; // we'll string copy this somewhere in the malloc'd portion
  pthread_rwlock_unlock(cacheset->lock);
  return 0;
  // return -1;
}

/* Add the given KEY, VALUE pair to CACHESET. Returns 0 if successful, else
 * returns a negative error code. Should evict elements if necessary to not
 * exceed CACHESET->elem_per_set total entries. */
int kvcacheset_put(kvcacheset_t *cacheset, char *key, char *value) {
  // OUR CODE HERE
  long index = hash(key);
  if (index >= cacheset->num_entries) {
    return -1; // i need to look up what type of error this rlly is in kvconstants.h
  }
  // i think we might need to check if these locks and unlocks were actually successful (on all functions obviously)
  pthread_rwlock_wrlock(cacheset->lock);
  // but. but. how do we know which index key should map to. What if they coincide. what.
  // do we only increment if they don't coincide. i'm confused now.
  if (cacheset->num_entries < cacheset->elem_per_set) {
    cacheset->num_entries++;
  }
  pthread_rwlock_unlock(cacheset->lock);
  return 0;
  // return -1;
}

/* Deletes the entry corresponding to KEY from CACHESET. Returns 0 if
 * successful, else returns a negative error code. */
int kvcacheset_del(kvcacheset_t *cacheset, char *key) {
  // OUR CODE HERE
  long index = hash(key);
  if (index >= cacheset->num_entries) {
    return -1; // i need to look up what type of error this rlly is in kvconstants.h
  }
  pthread_rwlock_wrlock(cacheset->lock);
  if (cacheset->num_entries == 0) {
    // i don't think it'll ever hit this case. but just in case i guess? this case should
    // be filtered initially outside the locks, if need be
  } else {
    // do some more stuff here
    cacheset->num_entries--;
  }
  pthread_rwlock_unlock(cacheset->lock);
  return 0;
  // return -1;
}

/* Completely clears this cache set. For testing purposes. */
void kvcacheset_clear(kvcacheset_t *cacheset) {
}
