#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "kvcache.h"
#include "kvconstants.h"
#include "kvcacheset.h"
#include "tester.h"
#include "kvstore.h"
#include <stdbool.h>
#include "utlist.h"

static kvcacheset_t *get_cache_set(kvcache_t *cache, char *key);

kvcache_t testcache;

/* This file contains four unit tests. Please see below for detail. */

/* Initialization of kvcache. */
int unit1_test_init() {
  kvcache_init(&testcache, 2, 2);
  return 0;
}

/* This test puts a key value pair in and then attempts to overide it,
 * and verifies that the overwritten value is the one that is now in the cache.
 */
int kvcache_put_overwrite() {
  kvcache_init(&testcache, 2, 2);
  char *retval;
  int ret;
  ret = kvcache_put(&testcache, "thiskey", "oldvalue");
  ret += kvcache_put(&testcache, "thiskey", "newvalue");
  ret += kvcache_get(&testcache, "thiskey", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "newvalue");
  ASSERT_EQUAL(ret, 0);
  free(retval);
  return 1;
}

/* This unit test puts multiple key value pairs in and then
 * attempts to override them both, and finally tries to get the new values.
 * It also tests what happens if you try to get a key that
 * does not exist in the cache (should return ERRNOKEY).
 */
int kvcache_put_overwrite_get_multiple() {
  char *retval;
  int ret;
  ret = kvcache_put(&testcache, "key1", "vsdfkl");
  ret += kvcache_put(&testcache, "key1", "sldkf");
  ret += kvcache_put(&testcache, "key2", "val2");
  ret += kvcache_put(&testcache, "key1", "val1");
  ret += kvcache_get(&testcache, "key1", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "val1");;
  free(retval);
  retval = NULL;
  int temp = kvcache_get(&testcache, "conrad", &retval);
  ASSERT_EQUAL(temp, ERRNOKEY);
  ASSERT_PTR_NULL(retval);
  ret += kvcache_get(&testcache, "key2", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "val2");
  free(retval);
  ret += kvcache_put(&testcache, "key2", "abcde");
  ret += kvcache_put(&testcache, "key1", "key2");
  ret += kvcache_get(&testcache, "key2", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "abcde");
  free(retval);
  ret += kvcache_get(&testcache, "key1", &retval);
  ASSERT_PTR_NOT_NULL(retval);
  ASSERT_STRING_EQUAL(retval, "key2");
  free(retval);
  ASSERT_EQUAL(ret, 0);
  return 1;
}

/* This unit test verifies that PUT operations set the initial refbits to false. */
int kvcache_check_initial_refbits() {
  kvcache_init(&testcache, 2, 2);
  struct kvcacheentry *elt;
  kvcacheset_t *cacheset;
  kvcache_put(&testcache, "key1", "vsdfkl");
  cacheset = get_cache_set(&testcache, "key1");
  DL_FOREACH(cacheset->head, elt) {
    if (strcmp(elt->key, "key1") == 0) {
      ASSERT(!elt->refbit);
      break;
    }
  }
  kvcache_put(&testcache, "keyanything", "somevalue");
  cacheset = get_cache_set(&testcache, "keyanything");
  DL_FOREACH(cacheset->head, elt) {
    if (strcmp(elt->key, "keyanything") == 0) {
      ASSERT(!elt->refbit);
      break;
    }
  }
  int i;
  for (i = 0; i < testcache.num_sets; i++) {
    kvcacheset_t currset = testcache.sets[i];
    struct kvcacheentry *elt;
    DL_FOREACH(currset.head, elt) {
      ASSERT(!elt->refbit);
    }
  }
  return 1;
}

/* Private helper method used for testing */
static kvcacheset_t *get_cache_set(kvcache_t *cache, char *key) {
  // OUR CODE HERE
  unsigned long index = hash(key) % cache->num_sets;
  return &cache->sets[index];
}

/* Our unit test to be printed onto the terminal for ease of seeing what fails and passes. */
test_info_t unit1_tests[] = {
  {"Testing PUT with simple overwriting", kvcache_put_overwrite},
  {"Testing PUT and GET with interplayed overwriting", kvcache_put_overwrite_get_multiple},
  {"Testing if initial refbits in a cacheset are turned OFF", kvcache_check_initial_refbits},
  NULL_TEST_INFO
};

suite_info_t unit_test1_suite = {"Unit 1 Tests: Extra KVCache Tests", unit1_test_init, NULL,
  unit1_tests};
