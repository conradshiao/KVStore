#include <stdbool.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include "kvconstants.h"
#include "kvcache.h"
#include "kvstore.h"
#include "kvmessage.h"
#include "kvserver.h"
#include "tpclog.h"
#include "socket_server.h"

// OUR CODE HERE
#include <string.h>
#include <stdlib.h>

#define PORT_NUM_LENGTH 16 // Used to help malloc our registration string with this server

static int copy_and_store_kvmessage(kvserver_t *server, kvmessage_t *msg);
static int rebuild_kvmessage(kvserver_t *server, logentry_t *e, bool put);

/* Initializes a kvserver. Will return 0 if successful, or a negative error
 * code if not. DIRNAME is the directory which should be used to store entries
 * for this server.  The server's cache will have NUM_SETS cache sets, each
 * with ELEM_PER_SET elements.  HOSTNAME and PORT indicate where SERVER will be
 * made available for requests.  USE_TPC indicates whether this server should
 * use TPC logic (for PUTs and DELs) or not. */
int kvserver_init(kvserver_t *server, char *dirname, unsigned int num_sets,
    unsigned int elem_per_set, unsigned int max_threads, const char *hostname,
    int port, bool use_tpc) {
  int ret;
  ret = kvcache_init(&server->cache, num_sets, elem_per_set);
  if (ret < 0) return ret;
  ret = kvstore_init(&server->store, dirname);
  if (ret < 0) return ret;
  if (use_tpc) {
    ret = tpclog_init(&server->log, dirname);
    if (ret < 0) return ret;
  }
  server->hostname = malloc(strlen(hostname) + 1);
  if (server->hostname == NULL)
    return ENOMEM;
  strcpy(server->hostname, hostname);
  server->port = port;
  server->use_tpc = use_tpc;
  server->max_threads = max_threads;
  server->handle = kvserver_handle;
  // OUR CODE HERE
  server->msg = NULL;
  server->state = TPC_READY;
  return 0;
}

/* Sends a message to register SERVER with a TPCMaster over a socket located at
 * SOCKFD which has previously been connected. Does not close the socket when
 * done. Returns -1 if an error was encountered.
 *
 * Checkpoint 2 only. */
int kvserver_register_master(kvserver_t *server, int sockfd) {
  // OUR CODE HERE
  kvmessage_t *reqmsg = (kvmessage_t *) malloc(sizeof(kvmessage_t));
  if (reqmsg == NULL) {
    return -1;
  }
  reqmsg->type = REGISTER;
  reqmsg->key = (char *) malloc((strlen(server->hostname) + 1) * sizeof(char));
  if (reqmsg->key == NULL) {
    kvmessage_free(reqmsg);
    return -1;
  }
  strcpy(reqmsg->key, server->hostname);
  reqmsg->value = (char *) malloc(sizeof(char) * PORT_NUM_LENGTH); // max number is 2^16 - 1
  if (reqmsg->value == NULL) {
    kvmessage_free(reqmsg);
    return -1;
  }
  sprintf(reqmsg->value, "%d", server->port);
  kvmessage_send(reqmsg, sockfd);
  kvmessage_t *response = kvmessage_parse(sockfd);
  int ret;
  if (!response || !response->message || strcmp(response->message, MSG_SUCCESS) != 0) {
    ret = -1;
  } else {
    ret = 0;
    server->state = TPC_READY;
  }
  kvmessage_free(response);
  return ret;
}

/* Attempts to get KEY from SERVER. Returns 0 if successful, else a negative
 * error code.  If successful, VALUE will point to a string which should later
 * be free()d.  If the KEY is in cache, take the value from there. Otherwise,
 * go to the store and update the value in the cache. */
int kvserver_get(kvserver_t *server, char *key, char **value) {
  // OUR CODE HERE
  int ret;
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (lock == NULL) return ERRKEYLEN;
  pthread_rwlock_rdlock(lock);
  if (kvcache_get(&server->cache, key, value) == 0) {
    pthread_rwlock_unlock(lock);
    return 0;
  }
  pthread_rwlock_unlock(lock);
  if ((ret = kvstore_get(&server->store, key, value)) < 0)
    return ret;
  pthread_rwlock_wrlock(lock);
  ret = kvcache_put(&server->cache, key, *value); // what happens if this is unsuccessful?
  pthread_rwlock_unlock(lock);
  return ret;
}

/* Checks if the given KEY, VALUE pair can be inserted into this server's
 * store. Returns 0 if it can, else a negative error code. */
int kvserver_put_check(kvserver_t *server, char *key, char *value) {
  // OUR CODE HERE
  return kvstore_put_check(&server->store, key, value);
}

/* Inserts the given KEY, VALUE pair into this server's store and cache. Access
 * to the cache should be concurrent if the keys are in different cache sets.
 * Returns 0 if successful, else a negative error code. */
int kvserver_put(kvserver_t *server, char *key, char *value) {
  // OUR CODE HERE
  int success;
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (lock == NULL) return ERRKEYLEN;
  pthread_rwlock_wrlock(lock);
  if ((success = kvcache_put(&server->cache, key, value)) < 0) {
    pthread_rwlock_unlock(lock);
    return success;
  }
  pthread_rwlock_unlock(lock);
  return kvstore_put(&server->store, key, value);
}

/* Checks if the given KEY can be deleted from this server's store.
 * Returns 0 if it can, else a negative error code. */
int kvserver_del_check(kvserver_t *server, char *key) {
  // OUR CODE HERE
  return kvstore_del_check(&server->store, key);
}

/* Removes the given KEY from this server's store and cache. Access to the
 * cache should be concurrent if the keys are in different cache sets. Returns
 * 0 if successful, else a negative error code. */
int kvserver_del(kvserver_t *server, char *key) {
  // OUR CODE HERE
  int ret;
  pthread_rwlock_t *lock = kvcache_getlock(&server->cache, key);
  if (lock == NULL) return ERRKEYLEN;
  pthread_rwlock_wrlock(lock);
  if ((ret = kvstore_del(&server->store, key)) < 0) {
    pthread_rwlock_unlock(lock);
    return ret;
  }
  pthread_rwlock_unlock(lock);
  kvcache_del(&server->cache, key); // if not in server's cache, that's okay
  return 0;
}

/* Returns an info string about SERVER including its hostname and port. */
char *kvserver_get_info_message(kvserver_t *server) {
  char info[1024], buf[256];
  time_t ltime = time(NULL);
  strcpy(info, asctime(localtime(&ltime)));
  sprintf(buf, "{%s, %d}", server->hostname, server->port);
  strcat(info, buf);
  char *msg = malloc(strlen(info));
  strcpy(msg, info);
  return msg;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assumes that the request should be handled as a TPC
 * message. This should also log enough information in the server's TPC log to
 * be able to recreate the current state of the server upon recovering from
 * failure. See the spec for details on logic and error messages.
 *
 * Checkpoint 2 only. */
void kvserver_handle_tpc(kvserver_t *server, kvmessage_t *reqmsg, kvmessage_t *respmsg) {
  // OUR CODE HERE
  int error = -1;
  bool initial_check = true;
  if (respmsg == NULL) {
    return;
  } else if (reqmsg == NULL || server == NULL) {
    goto unsuccessful_request;
  } else if (reqmsg->key == NULL) {
    if (reqmsg->type == GETREQ || reqmsg->type == PUTREQ || reqmsg->type == DELREQ) {
      goto unsuccessful_request;
    }
  } else if (reqmsg->value == NULL && reqmsg->type == PUTREQ) {
    goto unsuccessful_request;
  } else if (server->state == TPC_INIT) {
    initial_check = false;
    goto unsuccessful_request;
  }

  initial_check = false;
  switch (reqmsg->type) {

    case GETREQ:
      if ((error = kvserver_get(server, reqmsg->key, &reqmsg->value)) == 0) {
        respmsg->type = GETRESP;
        respmsg->key = reqmsg->key;
        respmsg->value = reqmsg->value;
      } else {
        goto unsuccessful_request;
      }
      break;

    case PUTREQ:
      if (server->state == TPC_WAIT) {
        initial_check = true;
        goto unsuccessful_request;
      }
      server->state = TPC_WAIT;

      tpclog_log(&server->log, PUTREQ, reqmsg->key, reqmsg->value);
      if ((error = kvserver_put_check(server, reqmsg->key, reqmsg->value)) == 0) {
        if ((error = copy_and_store_kvmessage(server, reqmsg)) == -1) {
          server->state = TPC_READY;
          goto unsuccessful_request;
        }
        respmsg->type = VOTE_COMMIT;
      } else {
        server->state = TPC_READY;
        respmsg->type = VOTE_ABORT;
        respmsg->message = GETMSG(error);
      }
      break;

    case DELREQ:
      if (server->state == TPC_WAIT) {
        initial_check = true;
        goto unsuccessful_request;
      }
      server->state = TPC_WAIT;

      tpclog_log(&server->log, DELREQ, reqmsg->key, reqmsg->value);
      if ((error = kvserver_del_check(server, reqmsg->key)) == 0) {
        if ((error = copy_and_store_kvmessage(server, reqmsg)) == -1) {
          server->state = TPC_READY;
          goto unsuccessful_request;
        }
        respmsg->type = VOTE_COMMIT;
      } else {
        server->state = TPC_READY;
        respmsg->type = VOTE_ABORT;
        respmsg->message = GETMSG(error); // need this field in tests.. specs forgot to say
      }
      break;

    case COMMIT:
      server->state = TPC_READY;
      tpclog_log(&server->log, COMMIT, NULL, NULL);
      if (server->msg->type == PUTREQ) {
        if ((error = kvserver_put(server, server->msg->key, server->msg->value)) < 0) {
          goto unsuccessful_request;
        }
        respmsg->type = ACK;
      } else { // type DELREQ
        if ((error = kvserver_del(server, server->msg->key)) < 0) {
          goto unsuccessful_request;
        }
        respmsg->type = ACK;
      }
      break;

    case ABORT:
      server->state = TPC_READY;
      tpclog_log(&server->log, ABORT, NULL, NULL);
      respmsg->type = ACK;
      break;

    default:
      respmsg->type = RESP;
      respmsg->message = ERRMSG_INVALID_REQUEST;
      break;  
  }

  return;

  /* All unsuccessful requests will be handled in the same manner. */
  unsuccessful_request:
    respmsg->type = RESP;
    respmsg->message = (initial_check) ? ERRMSG_INVALID_REQUEST : GETMSG(error);
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assumes that the request should be handled as a non-TPC
 * message. See the spec for details on logic and error messages. */
void kvserver_handle_no_tpc(kvserver_t *server, kvmessage_t *reqmsg, kvmessage_t *respmsg) {
  // OUR CODE HERE
  bool initial_check = true;
  if (respmsg == NULL) {
    return;
  } else if (reqmsg == NULL || server == NULL) {
    goto unsuccessful_request;
  } else if (reqmsg->key == NULL) {
    if (reqmsg->type == GETREQ || reqmsg->type == PUTREQ || reqmsg->type == DELREQ) {
      goto unsuccessful_request;
    }
  } else if (reqmsg->value == NULL && reqmsg->type == PUTREQ) {
    goto unsuccessful_request;
  } else if (server->state == TPC_INIT) {
    initial_check = false;
    goto unsuccessful_request;
  }

  initial_check = false;
  int error = -1;
  switch (reqmsg->type) {

    case GETREQ:
      if ((error = kvserver_get(server, reqmsg->key, &reqmsg->value)) == 0) {
        respmsg->type = GETRESP;
        respmsg->key = reqmsg->key;
        respmsg->value = reqmsg->value;
      } else {
        goto unsuccessful_request;
      }
      break;

    case PUTREQ:
      if ((error = kvserver_put(server, reqmsg->key, reqmsg->value)) == 0) {
        respmsg->type = RESP;
        respmsg->message = MSG_SUCCESS;
      } else {
        goto unsuccessful_request;
      }
      break;

    case DELREQ:
      if ((error = kvserver_del(server, reqmsg->key)) == 0) {
        respmsg->type = RESP;
        respmsg->message = MSG_SUCCESS;
      } else {
        goto unsuccessful_request;
      }
      break;

    case INFO:
      respmsg->type = INFO;
      respmsg->message = kvserver_get_info_message(server);
      break;

    default:
      respmsg->type = RESP;
      respmsg->message = ERRMSG_NOT_IMPLEMENTED;
      break;
  }

  return;

/* All unsuccessful requests will be handled in the same manner. */
  unsuccessful_request:
    respmsg->type = RESP;
    respmsg->message = (initial_check) ? ERRMSG_INVALID_REQUEST : GETMSG(error);
}
/* Generic entrypoint for this SERVER. Takes in a socket on SOCKFD, which
 * should already be connected to an incoming request. Processes the request
 * and sends back a response message.  This should call out to the appropriate
 * internal handler. */
void kvserver_handle(kvserver_t *server, int sockfd, void *extra) {
  kvmessage_t *reqmsg, *respmsg;
  respmsg = calloc(1, sizeof(kvmessage_t));
  reqmsg = kvmessage_parse(sockfd);
  void (*server_handler)(kvserver_t *server, kvmessage_t *reqmsg,
      kvmessage_t *respmsg);
  server_handler = server->use_tpc ?
    kvserver_handle_tpc : kvserver_handle_no_tpc;
  if (reqmsg == NULL) {
    respmsg->type = RESP;
    respmsg->message = ERRMSG_INVALID_REQUEST;
  } else {
    server_handler(server, reqmsg, respmsg);
  }
  kvmessage_send(respmsg, sockfd);
  if (reqmsg != NULL)
    kvmessage_free(reqmsg);
}

/* Restore SERVER back to the state it should be in, according to the
 * associated LOG. Must be called on an initialized SERVER. Only restores the
 * state of the most recent TPC transaction, assuming that all previous actions
 * have been written to persistent storage. Should restore SERVER to its exact
 * state; e.g. if SERVER had written into its log that it received a PUTREQ but
 * no corresponding COMMIT/ABORT, after calling this function SERVER should
 * again be waiting for a COMMIT/ABORT.  This should also ensure that as soon
 * as a server logs a COMMIT, even if it crashes immediately after (before the
 * KVStore has a chance to write to disk), the COMMIT will be finished upon
 * rebuild. The cache need not be the same as before rebuilding.
 *
 * Checkpoint 2 only. */
int kvserver_rebuild_state(kvserver_t *server) {
  if (server == NULL || server->state == TPC_INIT) {
    return -1;
  }
  tpclog_iterate_begin(&server->log);
  logentry_t *prev = NULL, *next = NULL;
  while (tpclog_iterate_has_next(&server->log)) {
    next = tpclog_iterate_next(&server->log);
    if (next->type == PUTREQ || next->type == DELREQ)
      prev = next;
  }
  if (prev == NULL && next == NULL) { // log was empty
    return 0;
  } else if (prev == NULL) {
    prev = next;
  }

  server->msg = (kvmessage_t *) malloc(sizeof(kvmessage_t));
  if (server->msg == NULL)
    return -1;

  if (next->type == COMMIT) {
    server->state = TPC_READY;
    if (prev->type == PUTREQ) {
      if (rebuild_kvmessage(server, prev, true) == -1) {
        return -1;
      }
      kvserver_put(server, server->msg->key, server->msg->value);
    } else if (prev->type == DELREQ) {
      if (rebuild_kvmessage(server, prev, false) == -1) {
        return -1;
      }
      kvserver_del(server, server->msg->key);
    }
  } else if (next->type == ABORT) { // might want to avoid commiting very first time with a one-time use bool
    server->state = TPC_READY;
    if (prev != NULL && rebuild_kvmessage(server, prev, prev->type == PUTREQ) == -1)
      return -1;
  } else {
    server->state = TPC_WAIT;
    if (rebuild_kvmessage(server, next, next->type == PUTREQ) == -1)
      return -1;
  }
  return tpclog_clear_log(&server->log);
}

static int rebuild_kvmessage(kvserver_t *server, logentry_t *e, bool put) {
  server->msg = (kvmessage_t *) malloc(sizeof(kvmessage_t));
  if (server->msg == NULL)
    return -1;
  server->msg->type = e->type;
  int key_size = strlen(e->data) + 1;
  server->msg->key = malloc(sizeof(char) * key_size);
  if (server->msg->key == NULL)
    return -1;
  strcpy(server->msg->key, e->data);
  if (put) {
    server->msg->value = malloc(sizeof(char) * (e->length - key_size));
    if (server->msg->value == NULL) {
      free(server->msg->key);
      return -1;
    }
    char *val = e->data;
    while (*val != '\0') val++;
    val++;
    strcpy(server->msg->value, val);   
  }
  return 0;
}

/* Deletes all current entries in SERVER's store and removes the store
 * directory.  Also cleans the associated log. */
int kvserver_clean(kvserver_t *server) {
  return kvstore_clean(&server->store);
}

// OUR CODE HERE
/* Copies and mallocs MSG and stores it in the SERVER->msg field so that
 * phase 2 can know what operation to do from phase 1. */
static int copy_and_store_kvmessage(kvserver_t *server, kvmessage_t *msg) {
  /* We don't need to worry about freeing mallocs, because we
     have kvmessage_free everytime at the beginning of this function. */
  kvmessage_free(server->msg);
  if ((server->msg = (kvmessage_t *) malloc(sizeof(kvmessage_t))) == NULL) {
    return -1;
  }

  kvmessage_t *m = server->msg;

  if (msg->key != NULL) {
    if ((m->key = (char *) malloc(sizeof(char) * (strlen(msg->key) + 1))) == NULL)
      return -1;
    strcpy(m->key, msg->key);
  } else {
    m->key = NULL;
  }

  if (msg->value != NULL) {
    if ((m->value = (char *) malloc(sizeof(char) * (strlen(msg->value) + 1))) == NULL)
      return -1;
    strcpy(m->value, msg->value);
  } else {
    m->value = NULL;
  }

  m->type = msg->type;
  return 0;
}
