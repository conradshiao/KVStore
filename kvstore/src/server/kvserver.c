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

#define PORT_NUM_LENGTH 16

static int copy_and_store_kvmessage(kvserver_t *server, kvmessage_t *msg);

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
  server->state = TPC_INIT;
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
    free(reqmsg);
    return -1;
  }
  strcpy(reqmsg->key, server->hostname);
  reqmsg->value = (char *) malloc(sizeof(char) * PORT_NUM_LENGTH); // max number is 2^16 - 1
  if (reqmsg->value == NULL) {
    free(reqmsg->key);
    free(reqmsg);
    return -1;
  }
  sprintf(reqmsg->value, "%d", server->port);
  kvmessage_send(reqmsg, sockfd);
  kvmessage_t *response;
  if ((response = kvmessage_parse(sockfd)) == NULL) {
    free(response);
    return -1;
  } else if (strcmp(MSG_SUCCESS, response->message) != 0) {
    free(response);
    return -1; // might change later to return specific error but how to get it.
  } else {
    free(response);
    printf("SHOULD HIT THIS CASE ALMOST ALWAYS.. RIGHT??????????????\n");
    server->state = TPC_READY;
    return 0;
  }
}

/* Attempts to get KEY from SERVER. Returns 0 if successful, else a negative
 * error code.  If successful, VALUE will point to a string which should later
 * be free()d.  If the KEY is in cache, take the value from there. Otherwise,
 * go to the store and update the value in the cache. */
int kvserver_get(kvserver_t *server, char *key, char **value) {
  // OUR CODE HERE
  int ret;
  if (kvcache_get(&server->cache, key, value) == 0)
    return 0;
  if ((ret = kvstore_get(&server->store, key, value)) < 0)
    return ret;
  return kvcache_put(&server->cache, key, *value); // what happens if this is unsuccessful?
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
  if ((success = kvcache_put(&server->cache, key, value)) < 0)
    return success;
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
  if ((ret = kvstore_del(&server->store, key)) < 0)
    return ret;
  kvcache_del(&server->cache, key);
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
 * failure.  See the spec for details on logic and error messages.
 *
 * Checkpoint 2 only. */
void kvserver_handle_tpc(kvserver_t *server, kvmessage_t *reqmsg, kvmessage_t *respmsg) {
  // OUR CODE HERE
  if (reqmsg == NULL || respmsg == NULL || server == NULL)
    return;

  int error;
  switch (reqmsg->type) {
    case GETREQ:
      if ((error = kvserver_get(server, reqmsg->key, &reqmsg->value)) == 0) {
        respmsg->type = GETRESP;
        respmsg->key = reqmsg->key;
        respmsg->value = reqmsg->value;
      } else {
        respmsg->type = RESP;
        respmsg->message = GETMSG(error);
      }
      break;

    case PUTREQ:
      tpclog_log(&server->log, PUTREQ, reqmsg->key, reqmsg->value);
      if ((error = kvserver_put_check(server, reqmsg->key, reqmsg->value)) == 0) {
        if ((error = copy_and_store_kvmessage(server, reqmsg)) == -1) {
          respmsg->type = RESP;
          respmsg->message = ERRMSG_GENERIC_ERROR;
          return;
        }
        if (respmsg->type == VOTE_COMMIT) {   // this is to check that prev REQ has been committed
          respmsg->type = RESP;
          respmsg->message = ERRMSG_INVALID_REQUEST;
          return; 
        }
        respmsg->type = VOTE_COMMIT;
      } else {
        respmsg->type = VOTE_ABORT;
        respmsg->message = GETMSG(error);
      }
      break;

    case DELREQ:
      tpclog_log(&server->log, DELREQ, reqmsg->key, reqmsg->value);
      if ((error = kvserver_del_check(server, reqmsg->key)) == 0) {
        if ((error = copy_and_store_kvmessage(server, reqmsg)) == -1) {
          respmsg->type = RESP;
          respmsg->message = ERRMSG_GENERIC_ERROR;
          return;
        }
        if (respmsg->type == VOTE_COMMIT) {
          respmsg->type = RESP;
          respmsg->message = ERRMSG_INVALID_REQUEST;
          return; 
        }
        respmsg->type = VOTE_COMMIT;
      } else {
        respmsg->type = VOTE_ABORT;
        respmsg->message = GETMSG(error); // need this field in tests.. specs forgot to say
      }
      break;

    case COMMIT:
      tpclog_log(&server->log, COMMIT, reqmsg->key, reqmsg->value);
      // respmsg->type = ACK;
      if (server->msg->type == PUTREQ) {
        if ((error = kvserver_put(server, server->msg->key, server->msg->value)) == 0) {
          respmsg->type = ACK;
        } // else it's an error and we don't want to send.. anything?
      } else if (server->msg->type == DELREQ) {
        if ((error = kvserver_del(server, server->msg->key)) == 0) {
          respmsg->type = ACK;
        } // same as above case...
      } else {
        printf("\nUNEXPECTED ERROR: WHOA WHOA WHO WAHO WHOA SLDKF;ASDFJAL;SFD WHY DOE\n");
      }
      break;

    case ABORT:
      tpclog_log(&server->log, ABORT, reqmsg->key, reqmsg->value);
      respmsg->type = ACK;
      break;

    default:
      respmsg->type = RESP;
      respmsg->message = ERRMSG_INVALID_REQUEST;
      break;  
  }
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assumes that the request should be handled as a non-TPC
 * message. See the spec for details on logic and error messages. */
void kvserver_handle_no_tpc(kvserver_t *server, kvmessage_t *reqmsg, kvmessage_t *respmsg) {
  // OUR CODE HERE
  if (reqmsg == NULL || respmsg == NULL || server == NULL)
    return;

  int error;
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
    respmsg->message = GETMSG(error); 
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
  // OUR CODE HERE
  /* The tpcmaster needs to keep this kvmessage -- freeing is on him now. */
  // if (reqmsg != NULL)
  //   kvmessage_free(reqmsg);
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
  return -1;
}

/* Deletes all current entries in SERVER's store and removes the store
 * directory.  Also cleans the associated log. */
int kvserver_clean(kvserver_t *server) {
  return kvstore_clean(&server->store);
}

/* Copies and mallocs MSG and stores it in the SERVER->msg field so that
 * phase 2 can know what operation to do from phase 1. */
static int copy_and_store_kvmessage(kvserver_t *server, kvmessage_t *msg) {
  // OUR CODE HERE
  free(server->msg);
  server->msg = (kvmessage_t *) malloc(sizeof(kvmessage_t));
  if (server->msg == NULL)
    return -1;

  if (msg->key)
    server->msg->key = (char *) malloc((strlen(msg->key) + 1) * sizeof(char));
  if (msg->value)
    server->msg->value = (char *) malloc((strlen(msg->value) + 1) * sizeof(char));
  if (server->msg->key == NULL || server->msg->value == NULL)
    return -1;

  if (msg->key)
    strcpy(server->msg->key, msg->key);
  else
    server->msg->key = NULL;

  if (msg->value)
    strcpy(server->msg->value, msg->value);
  else
    server->msg->value = NULL;

  server->msg->type = msg->type;
  return 0;
}
