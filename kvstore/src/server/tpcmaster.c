#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include "kvconstants.h"
#include "kvmessage.h"
#include "socket_server.h"
#include "time.h"
#include "tpcmaster.h"

// OUR CODE HERE
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define MAX_INFOLINE_LENGTH 256

#define TIMEOUT_SECONDS 2

static int port_cmp(tpcslave_t *a, tpcslave_t *b);

static void phase1(tpcmaster_t *master, tpcslave_t *slave,
                   kvmessage_t *reqmsg, callback_t callback);
static void phase2(tpcmaster_t *master, tpcslave_t *slave,
                   kvmessage_t *reqmsg, callback_t callback);

/* Initializes a tpcmaster. Will return 0 if successful, or a negative error
 * code if not. SLAVE_CAPACITY indicates the maximum number of slaves that
 * the master will support. REDUNDANCY is the number of replicas (slaves) that
 * each key will be stored in. The master's cache will have NUM_SETS cache sets,
 * each with ELEM_PER_SET elements. */
int tpcmaster_init(tpcmaster_t *master, unsigned int slave_capacity,
    unsigned int redundancy, unsigned int num_sets, unsigned int elem_per_set) {
  int ret;
  ret = kvcache_init(&master->cache, num_sets, elem_per_set);
  if (ret < 0) return ret;
  ret = pthread_rwlock_init(&master->slave_lock, NULL);
  if (ret < 0) return ret;
  master->slave_count = 0;
  master->slave_capacity = slave_capacity;
  if (redundancy > slave_capacity) {
    master->redundancy = slave_capacity;
  } else {
    master->redundancy = redundancy;
  }
  master->slaves_head = NULL;
  master->handle = tpcmaster_handle;
  // OUR CODE HERE
  master->client_req = NULL;
  master->sorted = false;
  master->state = TPC_INIT;
  return 0;
}

/* Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
 * adapted from the Java builtin String.hashcode().
 * DO NOT CHANGE THIS FUNCTION. */
int64_t hash_64_bit(char *s) {
  int64_t h = 1125899906842597LL;
  int i;
  for (i = 0; s[i] != 0; i++) {
    h = (31 * h) + s[i];
  }
  return h;
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Assigns an ID to the slave by hashing a string in the
 * format PORT:HOSTNAME, then tries to add its info to the MASTER's list of
 * slaves. If the slave is already in the list, do nothing (success).
 * There can never be more slaves than the MASTER's slave_capacity. RESPMSG
 * will have MSG_SUCCESS if registration succeeds, or an error otherwise.
 *
 * Checkpoint 2 only. */
void tpcmaster_register(tpcmaster_t *master, kvmessage_t *reqmsg, kvmessage_t *respmsg) {
  // OUR CODE HERE
  if (respmsg == NULL || reqmsg == NULL)
    return;

  char *port = reqmsg->value;
  char *hostname = reqmsg->key;
  int port_strlen = strlen(reqmsg->value);
  int hostname_strlen = strlen(reqmsg->key);

  /* Need to add 2, one for null terminator and one for the ":". */
  char *format_string = (char *) malloc((port_strlen + hostname_strlen + 2) * sizeof(char));
  if (format_string == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }

  strcpy(format_string, port);
  strcat(format_string, ":");
  strcat(format_string, hostname);
  int64_t hash_val = hash_64_bit(format_string);
  free(format_string);

  pthread_rwlock_wrlock(&master->slave_lock);

  /* Check to see if slave is still in the list. */
  tpcslave_t *elt;
  CDL_FOREACH(master->slaves_head, elt) {
    if (elt->id > hash_val) {
      break;
    }
    if (elt->id == hash_val) { // slave already in list
      respmsg->message = MSG_SUCCESS;
      goto unlock;
    }
  }

  if (master->slave_count == master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto unlock;
  } else {
    master->slave_count++;
    if (master->slave_count == master->slave_capacity) {
      master->state = TPC_READY;
    }
  }

  tpcslave_t *slave = (tpcslave_t *) malloc(sizeof(tpcslave_t));
  if (slave == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto unlock;
  }

  /* Filling in appropriate fields of tpcslave_t */
  slave->id = hash_val;
  slave->host = (char *) malloc((hostname_strlen + 1) * sizeof(char));
  if (slave->host == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto free_slave;
  }
  strcpy(slave->host, hostname);
  char *ptr;
  int num = strtol(port, &ptr, 10);
  // if (*ptr != NULL) { // unsuccessful conversion
  if (*ptr) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto free_slave_host;
  }
  slave->port = num;
  CDL_PREPEND(master->slaves_head, slave);
  CDL_SORT(master->slaves_head, port_cmp);
  master->sorted = true;
  respmsg->message = MSG_SUCCESS;
  return;

  free_slave_host:
    free(slave->host);
  free_slave:
    free(slave);
  unlock:
    pthread_rwlock_unlock(&master->slave_lock);
}

/* Comparator function to be used to sort our DL list of tpcslave_t slaves. */
static int port_cmp(tpcslave_t *a, tpcslave_t *b) {
  return a->id > b->id;
}

/* Hashes KEY and finds the first slave that should contain it.
 * It should return the first slave whose ID is greater than the
 * KEY's hash, and the one with lowest ID if none matches the
 * requirement.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_primary(tpcmaster_t *master, char *key) {
  // OUR CODE HERE
  pthread_rwlock_wrlock(&master->slave_lock);

  if (!master->sorted) {
    CDL_SORT(master->slaves_head, port_cmp);
    master->sorted = true;
  }

  int64_t hash_val = hash_64_bit(key);
  tpcslave_t *elt;
  if (master->slaves_head->prev->id < hash_val) { // highest slave ID < hash_val
    elt = master->slaves_head;
  } else {
    CDL_FOREACH(master->slaves_head, elt) {
      if (elt->id > hash_val) {
        break;
      }
    }
  }

  pthread_rwlock_unlock(&master->slave_lock);
  return elt;
}

/* Returns the slave whose ID comes after PREDECESSOR's, sorted
 * in increasing order.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_successor(tpcmaster_t *master, tpcslave_t *predecessor) {
  // OUR CODE HERE: i'm assuming we're guaranteed that predecessor is always in our list...
  
  pthread_rwlock_wrlock(&master->slave_lock);

  if (!master->sorted) {
    CDL_SORT(master->slaves_head, port_cmp);
    master->sorted = true;
  }

  tpcslave_t *elt;
  // bool saw_successor = false;
  CDL_SEARCH_SCALAR(master->slaves_head, elt, id, predecessor->id);
  if (elt) {
    elt = elt->next;
    pthread_rwlock_unlock(&master->slave_lock);
    return elt;
  } else {
    printf("\n\n\n\n\n\n\n");
    printf("WHOA WHOA WHOA HWOA HOLD UP NOW. WHY. HALP. PREDECESSOR ISN'T IN LIST\n");
    printf("\n");
    pthread_rwlock_unlock(&master->slave_lock);
    return NULL;
  }

}

/* Handles an incoming GET request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs.
 *
 * Checkpoint 2 only. */
void tpcmaster_handle_get(tpcmaster_t *master, kvmessage_t *reqmsg,
                          kvmessage_t *respmsg) {
  // OUR CODE HERE
  // respmsg->message = ERRMSG_NOT_IMPLEMENTED;
  if (respmsg == NULL || reqmsg == NULL)
    return;

  char *value;
  kvmessage_t *received_response;
  if (kvcache_get(&master->cache, reqmsg->key, &value) == 0) {
    respmsg->type = GETRESP;
    respmsg->key = reqmsg->key;
    respmsg->value = value;
    respmsg->message = MSG_SUCCESS;
  } else {
    tpcslave_t *slave = tpcmaster_get_primary(master, reqmsg->key);
    int fd;
    if ((fd = connect_to(slave->host, slave->port, TIMEOUT_SECONDS)) == -1) {
      goto generic_error;
    }
    kvmessage_send(reqmsg, fd);
    received_response = kvmessage_parse(fd);
    if (received_response == NULL) {
      goto generic_error;
    }
    if (strcmp(received_response->message, MSG_SUCCESS) == 0) {
      respmsg->key = (char *) malloc((strlen(received_response->key) + 1) * sizeof(char));
      respmsg->value = (char *) malloc((strlen(received_response->value) + 1) * sizeof(char));
      if (respmsg->key == NULL || respmsg->value == NULL) {
        goto free_fields;
      }
      //FIXME: README: So okay. I'm doing this cuz piazza says so. but i think the piazza guy is wrong... lulz
      /* See if i can modify this later to simply set respmsg equal to received_response... i think i can now. */
      strcpy(respmsg->key, received_response->key);
      strcpy(respmsg->value, received_response->value);
      respmsg->type = GETRESP;
      respmsg->message = MSG_SUCCESS;
      kvcache_put(&master->cache, respmsg->key, respmsg->value);
    } else {
      respmsg->type = RESP;
      respmsg->message = received_response->message;
    }
    free(received_response);
  }
  //printf("response: %s\n", respmsg->message);
  //FIXME: README: Do we have to free the kvmessage_parse()??? I think we can here actually...
  return;

  free_fields:
    free(received_response); // I think here?
    free(respmsg->key);
    free(respmsg->value);
  generic_error:
    respmsg->type = RESP;
    respmsg->message = ERRMSG_GENERIC_ERROR;
}

/* Handles an incoming TPC request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Implements the TPC algorithm, polling all the slaves
 * for a vote first and sending a COMMIT or ABORT message in the second phase.
 * Must wait for an ACK from every slave after sending the second phase messages. 
 * 
 * The CALLBACK field is used for testing purposes. You MUST include the following
 * calls to the CALLBACK function whenever CALLBACK is not null, or you will fail
 * some of the tests:
 * - During both phases of contacting slaves, whenever a slave cannot be reached (i.e. you
 *   attempt to connect and receive a socket fd of -1), call CALLBACK(slave), where
 *   slave is a pointer to the tpcslave you are attempting to contact.
 * - Between the two phases, call CALLBACK(NULL) to indicate that you are transitioning
 *   between the two phases.  
 * 
 * Checkpoint 2 only. */
void tpcmaster_handle_tpc(tpcmaster_t *master, kvmessage_t *reqmsg,
                          kvmessage_t *respmsg, callback_t callback) {

  
  if (reqmsg == NULL || respmsg == NULL)
    return; 
  assert (reqmsg->type == PUTREQ || reqmsg->type == DELREQ);
  tpcslave_t *primary = tpcmaster_get_primary(master, reqmsg->key);
  tpcslave_t *iter = primary;
  master->state = TPC_COMMIT;
  int i;
  for (i = 0; i < master->redundancy; i++) {
    phase1(master, iter, reqmsg, callback);
    if ((iter = iter->next) == NULL) {
      iter = master->slaves_head;
    }
  }

  if (callback != NULL) {
    callback(NULL);
  }

  kvmessage_t globalmsg;
  memset(&globalmsg, 0, sizeof(kvmessage_t));
  if (master->state == TPC_COMMIT) { // we commit during phase1 function... yes?
    globalmsg.type = COMMIT;
  } else { 
    assert(master->state == TPC_ABORT);
    globalmsg.type = ABORT;
  }
  iter = primary;
  for (i = 0; i < master->redundancy; i++) {
    phase2(master, iter, &globalmsg, callback);
    if ((iter = iter->next) == NULL) {
      iter = master->slaves_head;
    }
  }
  if (master->state == TPC_COMMIT) {
    respmsg->type = RESP;
    respmsg->message = MSG_SUCCESS;
  } else { 
    assert(master->state == TPC_ABORT);
    respmsg->type = RESP;
    respmsg->message = GETMSG(-1); // which error?
  }  
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Provides information about the slaves that are
 * currently alive.
 *
 * Checkpoint 2 only. */
void tpcmaster_info(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  char buf[256];
  char *info = (char *) malloc((master->slave_count * MAX_INFOLINE_LENGTH + 256) * sizeof(char));
  time_t ltime = time(NULL);
  strcpy(info, asctime(localtime(&ltime)));
  strcat(info, "Slaves:");
  tpcslave_t *elt;
  pthread_rwlock_rdlock(&master->slave_lock);
  CDL_FOREACH(master->slaves_head, elt) {
    sprintf(buf, "\n{%s, %d}", elt->host, elt->port);
    strcat(info, buf);
  }
  pthread_rwlock_unlock(&master->slave_lock);
  respmsg->message = info;
}

/* Generic entrypoint for this MASTER. Takes in a socket on SOCKFD, which
 * should already be connected to an incoming request. Processes the request
 * and sends back a response message.  This should call out to the appropriate
 * internal handler. */
void tpcmaster_handle(tpcmaster_t *master, int sockfd, callback_t callback) {
  kvmessage_t *reqmsg, respmsg;
  reqmsg = kvmessage_parse(sockfd);
  memset(&respmsg, 0, sizeof(kvmessage_t));
  respmsg.type = RESP;
  if (reqmsg->key != NULL) {
    respmsg.key = calloc(1, strlen(reqmsg->key));
    strcpy(respmsg.key, reqmsg->key);
  }

  // OUR CODE HERE
  master->client_req = reqmsg;

  if (reqmsg->type == INFO) {
    tpcmaster_info(master, reqmsg, &respmsg);
  } else if (reqmsg == NULL || reqmsg->key == NULL) {
    respmsg.message = ERRMSG_INVALID_REQUEST;
  } else if (reqmsg->type == REGISTER) {
    tpcmaster_register(master, reqmsg, &respmsg);
  } else if (reqmsg->type == GETREQ) {
    tpcmaster_handle_get(master, reqmsg, &respmsg);
  } else {
    tpcmaster_handle_tpc(master, reqmsg, &respmsg, callback);
  }
  kvmessage_send(&respmsg, sockfd);
  kvmessage_free(reqmsg);
  //FIXME: README: So will said we have to move kvmessage_free() if we want to 
  // save it as a field. but... i think the skeleton code looks right already though.
  // doesn't tpcmaster_handle_tpc() finish phase 1 and 2, meaning kvmessage_free() works perfectly fine here?
  //
  // else.. where else do we free it?
  if (respmsg.key != NULL)
    free(respmsg.key);
}

/* Completely clears this TPCMaster's cache. For testing purposes. */
void tpcmaster_clear_cache(tpcmaster_t *tpcmaster) {
  kvcache_clear(&tpcmaster->cache);
}


/* Send and receive message to and from slave in phase 1 of TPC */
static void phase1(tpcmaster_t *tpcmaster, tpcslave_t *slave, kvmessage_t *reqmsg, callback_t callback) {
  int fd = connect_to(slave->host, slave->port, 2);
  if (fd == -1) {
    if (callback != NULL) {
      callback(slave);
    }
    return;
  }
  kvmessage_send(reqmsg, fd);
  kvmessage_t *temp = kvmessage_parse(fd);
  if (temp->type == VOTE_ABORT) {
    tpcmaster->state = TPC_ABORT;
  }
  // else if (temp->type == VOTE_COMMIT)
  //   do nothing.. right?
}

/* Send and receive message to and from slave in phase 2 of TPC */
static void phase2(tpcmaster_t *tpcmaster, tpcslave_t *slave, kvmessage_t *reqmsg, callback_t callback) {
  // OUR CODE HERE
  int fd = connect_to(slave->host, slave->port, 2);
  if (fd == -1) {
    if (callback != NULL) {
      callback(slave);
    }
    return;
  }
  while (true) {
    kvmessage_send(reqmsg, fd);
    kvmessage_t *response = kvmessage_parse(fd);
    if (response->type == ACK)
      break;
  }

}
