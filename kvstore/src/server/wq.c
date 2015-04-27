#include <stdlib.h>
#include "wq.h"
#include "kvconstants.h"
#include "utlist.h"
// OUR CODE HERE
#include <semaphore.h>
#include <stdbool.h>

/* Initializes a work queue WQ. Sets up any necessary synchronization constructs. */
void wq_init(wq_t *wq) {
  wq->head = NULL;
  sem_t atomic, empty;
  sem_init(&atomic, 0, 1); // 2nd arg = 0 --> semaphore can be used only by this process. I think in our case can be 0 or 1.
  sem_init(&empty, 0, 0);
  wq->atomic = atomic;
  wq->empty = empty;
}

/* Remove an item from the WQ. Currently, this immediately attempts
 * to remove the item at the head of the list, and will fail if there are
 * no items in the list.
 *
 * It is your task to make it so that this function will wait until the queue
 * contains at least one item, then remove that item from the list and
 * return it. */
void *wq_pop(wq_t *wq) {
  if (wq->head == NULL) {
    sem_wait(&wq->empty);
  }
  sem_wait(&wq->atomic);
  void *job = wq->head->item;
  DL_DELETE(wq->head,wq->head);
  sem_post(&wq->atomic);
  return job;
}

/* Add ITEM to WQ. Currently, this just adds ITEM to the list.
 *
 * It is your task to perform any necessary operations to properly
 * perform synchronization. */
void wq_push(wq_t *wq, void *item) {
  sem_wait(&wq->atomic);
  bool was_empty = (wq->head == NULL); // i think i'm doing this dumbly, but it should work. i think.
  wq_item_t *wq_item = calloc(1, sizeof(wq_item_t));
  wq_item->item = item;
  DL_APPEND(wq->head, wq_item);
  sem_post(&wq->atomic);
  if (was_empty) {
    sem_post(&wq->empty);
  }
}
