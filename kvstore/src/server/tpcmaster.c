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
#include <stdlib.h>
#include <string.h>

#define MAX_INFOLINE_LENGTH 256

#define TIMEOUT_SECONDS 2

static int port_cmp(tpcslave_t *a, tpcslave_t *b);
static void sort_slaves_list(tpcmaster_t *master, bool force_sort);
static void update_check_master_state(tpcmaster_t *master);
static int copy_and_store_kvmessage(tpcmaster_t *master, kvmessage_t *msg);

static void phase1(tpcmaster_t *master, tpcslave_t *slave,
                   kvmessage_t *reqmsg, callback_t callback);
static void phase2(tpcslave_t *slave, kvmessage_t *reqmsg, callback_t callback);

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
  master->err_msg = NULL;
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

// OUR CODE HERE
/* Sorts the slaves list of master and updates its "sorted" field. Checks to see if
   we need to sort based on FORCE_SORT or if the "sorted" field is already false. */
static void sort_slaves_list(tpcmaster_t *master, bool force_sort) {
  if (force_sort || !master->sorted) {
    CDL_SORT(master->slaves_head, port_cmp);
    master->sorted = true;
  }
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
  if (respmsg == NULL) {
    return;
  }
  respmsg->type = RESP; // error or not, message type will be RESP
  /* For last check: strtol on empty strings convert strings to 0. */
  if (reqmsg == NULL || master == NULL || reqmsg->value == NULL ||
                        reqmsg->key == NULL|| strcmp(reqmsg->value, "") == 0) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }

  /* These variables used to determine if we want to reset master's original state,
    if we use goto upon encountering any error. */
  tpc_state_t orig_state = master->state;

  char *port = reqmsg->value;
  char *hostname = reqmsg->key;
  int port_strlen = strlen(port);
  int hostname_strlen = strlen(hostname);

  /* Need to add 2, one for null terminator and one for ':' */
  char *format_string = (char *) malloc(sizeof(char) * (port_strlen + hostname_strlen + 2));
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
  CDL_SEARCH_SCALAR(master->slaves_head, elt, id, hash_val);
  if (elt != NULL) { // slave found to be already in list
    respmsg->message = MSG_SUCCESS;
    goto unlock;
  }

  if (master->slave_count == master->slave_capacity) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto unlock;
  } else {
    master->slave_count++;
    update_check_master_state(master);
  }

  tpcslave_t *slave = (tpcslave_t *) malloc(sizeof(tpcslave_t));
  if (slave == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto unlock;
  }

  /* Filling in appropriate fields of tpcslave_t */
  slave->id = hash_val;
  slave->host = (char *) malloc(sizeof(char) * (hostname_strlen + 1));
  if (slave->host == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto free_slave;
  }
  strcpy(slave->host, hostname);

  char *ptr;
  int num = strtol(port, &ptr, 10);
  if (*ptr) { // check for unsuccessful conversion
    respmsg->message = ERRMSG_GENERIC_ERROR;
    goto free_slave_host;
  }
  slave->port = num;
  CDL_PREPEND(master->slaves_head, slave);
  sort_slaves_list(master, true);
  respmsg->message = MSG_SUCCESS;

  return;

  free_slave_host:
    free(slave->host);
  free_slave:
    free(slave);
  unlock:
    pthread_rwlock_unlock(&master->slave_lock);
  /* reset to initializing state if registering last server ran into error */
  master->state = orig_state;
}

// OUR CODE HERE
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

  sort_slaves_list(master, false);

  int64_t hash_val = hash_64_bit(key);
  tpcslave_t *elt;
  if (master->slaves_head->prev->id < hash_val) { // max slave ID < hash_val
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
  // OUR CODE HERE
  pthread_rwlock_wrlock(&master->slave_lock);


  sort_slaves_list(master, false);

  tpcslave_t *elt;
  CDL_SEARCH_SCALAR(master->slaves_head, elt, id, predecessor->id);

  assert(elt != NULL); // check to guarantee that predecessor is always in our list
  elt = elt->next;

  pthread_rwlock_unlock(&master->slave_lock);
  return elt;
}

/* Handles an incoming GET request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs.
 *
 * Checkpoint 2 only. */
void tpcmaster_handle_get(tpcmaster_t *master, kvmessage_t *reqmsg,
                          kvmessage_t *respmsg) {
  // OUR CODE HERE
  int error = -1;
  if (respmsg == NULL) {
    return;
  } else if (master == NULL || reqmsg == NULL) {
    goto generic_error;
  } else if (reqmsg->key == NULL) {
    error = ERRNOKEY; //FIXME: should this be errnokey or a generic error?
    goto generic_error;
  }

  char *value;
  kvmessage_t *received_response;
  if (kvcache_get(&master->cache, reqmsg->key, &value) == 0) {
    // printf("MERP\n");
    respmsg->type = GETRESP;

    if ((respmsg->key = (char *) malloc(sizeof(char) * (strlen(reqmsg->key) + 1))) == NULL)
      goto generic_error;
    strcpy(respmsg->key, reqmsg->key);

    if ((respmsg->value = (char *) malloc(sizeof(char) * (strlen(value) + 1))) == NULL)
      goto generic_error;

    strcpy(respmsg->value, value);
    free(value);
    respmsg->message = MSG_SUCCESS;
  } else {
    // printf("I am here\n");
    tpcslave_t *slave = tpcmaster_get_primary(master, reqmsg->key);
    int fd;
    bool successful_connection = false;
    int i;
    for (i = 0; i < master->redundancy; i++) {
      if ((fd = connect_to(slave->host, slave->port, TIMEOUT_SECONDS)) == -1) {
        slave = tpcmaster_get_successor(master, slave);
      } else {
        successful_connection = true;
        break;
      }
    }
    if (!successful_connection) {
      goto generic_error;
    }
    kvmessage_send(reqmsg, fd);
    received_response = kvmessage_parse(fd);
    close(fd);
    if (received_response == NULL) {
      goto generic_error;
    }
    // printf("\ncococococ hi hi\n");
    // if (received_response->message == NULL) {
    //   printf("message is null?\n\n");
    // }
    // if (received_response->type != NULL) {
    //   printf("NOT NULL TYPE!!\n");
    //   if (received_response->type == GETRESP)
    //     printf("1");
    //   if (received_response->type == RESP)
    //     printf("2");
    // }
    if (received_response->message == NULL || strcmp(received_response->message, MSG_SUCCESS) != 0) {
      printf("\nconrad what's up yo in the else case\n");
      respmsg->type = RESP;
      respmsg->message = received_response->message;
    // if (received_response->type != GETRESP) {
    //   respmsg->type = RESP;
    //   respmsg->message = received_response->message;
    } else {
      printf("\ns;adkfja;sldfjal; what's up yo\n");
      respmsg->key = (char *) malloc(sizeof(char) * (strlen(received_response->key) + 1));
      respmsg->value = (char *) malloc(sizeof(char) * (strlen(received_response->value) + 1));
      if (respmsg->key == NULL || respmsg->value == NULL) {
        goto free_fields;
      }
      strcpy(respmsg->key, received_response->key);
      strcpy(respmsg->value, received_response->value);
      respmsg->type = GETRESP;
      respmsg->message = MSG_SUCCESS;
      kvcache_put(&master->cache, respmsg->key, respmsg->value);
    }
    // if (strcmp(received_response->message, MSG_SUCCESS) == 0) {
    //   printf("\nKaren what's up yo\n");
    //   respmsg->key = (char *) malloc(sizeof(char) * (strlen(received_response->key) + 1));
    //   respmsg->value = (char *) malloc(sizeof(char) * (strlen(received_response->value) + 1));
    //   if (respmsg->key == NULL || respmsg->value == NULL) {
    //     goto free_fields;
    //   }
    //   strcpy(respmsg->key, received_response->key);
    //   strcpy(respmsg->value, received_response->value);
    //   respmsg->type = GETRESP;
    //   respmsg->message = MSG_SUCCESS;
    //   kvcache_put(&master->cache, respmsg->key, respmsg->value);
    // } else {
    //       printf("\nKaren what's up yo in the else case\n");
    //   respmsg->type = RESP;
    //   respmsg->message = received_response->message;
    // }
    free(received_response);
  }

  return;

  free_fields:
    free(received_response);
    free(respmsg->key);
    free(respmsg->value);
  generic_error:
    respmsg->type = RESP;
    respmsg->message = GETMSG(error);
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
  // OUR CODE HERE
  int error = -1;
  if (respmsg == NULL) {
    return;
  } else if (master == NULL) {
    goto input_error;
  }
  update_check_master_state(master);
  if (reqmsg == NULL) {
    goto input_error;
  } else if (reqmsg->key == NULL) {
    error = ERRNOKEY;
    goto input_error;
  } else if (master->state == TPC_INIT ||
              (reqmsg->type == PUTREQ && reqmsg->value == NULL) ||
              (reqmsg->type != PUTREQ && reqmsg->type != DELREQ))
  {
    goto input_error;
  }

  /* Phase 1 of TPC being set up and executed here. */
  tpcslave_t *primary = tpcmaster_get_primary(master, reqmsg->key);
  tpcslave_t *iter = primary;
  master->state = TPC_COMMIT; // initialized here, will be updated if a server fails
  int i;
  for (i = 0; i < master->redundancy; i++) {
    phase1(master, iter, reqmsg, callback);
    iter = tpcmaster_get_successor(master, iter);
  }

  /* Necessary as described by documentation between phase 1 and 2. */
  if (callback != NULL) {
    callback(NULL);
  }

  /* Setting up the global message that master will send to slave servers. */
  kvmessage_t globalmsg;
  memset(&globalmsg, 0, sizeof(kvmessage_t));
  if (master->state == TPC_COMMIT) {
    globalmsg.type = COMMIT;
  } else { 
    globalmsg.type = ABORT;
  }

  /* Phase 2 of TPC being set up and executed here. */
  iter = primary;
  for (i = 0; i < master->redundancy; i++) {
    phase2(iter, &globalmsg, callback);
    iter = tpcmaster_get_successor(master, iter);
  }

  respmsg->type = RESP;
  if (master->state == TPC_COMMIT) {
    respmsg->message = MSG_SUCCESS;
  } else { 
    assert(master->state == TPC_ABORT);
    respmsg->message = master->err_msg;
  }
  master->state = TPC_READY;

  return;

  input_error:
    respmsg->type = RESP;
    respmsg->message = GETMSG(error);
}

/* Handles an incoming kvmessage REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs. Provides information about the slaves that are
 * currently alive.
 *
 * Checkpoint 2 only. */
void tpcmaster_info(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  // OUR CODE HERE
  char buf[256];
  char *info = (char *) malloc((master->slave_count * MAX_INFOLINE_LENGTH + 256) * sizeof(char));
  if (info == NULL) {
    respmsg->type = RESP;
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }
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
  int err;
  if ((err = copy_and_store_kvmessage(master, reqmsg)) == -1) {
    respmsg.message = ERRMSG_GENERIC_ERROR; // type is already set above to RESP
    return;
  }

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
  if (respmsg.key != NULL)
    free(respmsg.key);
}

/* Completely clears this TPCMaster's cache. For testing purposes. */
void tpcmaster_clear_cache(tpcmaster_t *tpcmaster) {
  kvcache_clear(&tpcmaster->cache);
}

/* Send and receive message to and from slave in phase 1 of TPC */
static void phase1(tpcmaster_t *master, tpcslave_t *slave,
                   kvmessage_t *reqmsg, callback_t callback) {
  // OUR CODE HERE
  int fd = connect_to(slave->host, slave->port, 2);
  if (fd == -1) {
    if (callback != NULL) {
      callback(slave);
    }
    return;
  }
  kvmessage_send(reqmsg, fd);
  kvmessage_t *temp = kvmessage_parse(fd);
  close(fd);
  if (temp == NULL || temp->type == VOTE_ABORT) {
    master->state = TPC_ABORT;
    /* okay to do right before free-ing because "message" is literal string from kvconstants.h */
    master->err_msg = temp->message;
  }
  free(temp);
}

/* Send and receive message to and from slave in phase 2 of TPC */
static void phase2(tpcslave_t *slave, kvmessage_t *reqmsg, callback_t callback) {
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
    if (response->type == ACK) {
      free(response);
      break;
    }
    free(response);
  }
  close(fd);
}

// OUR CODE HERE
/* Checks to see if state of the master has moved on from initialization. */
static void update_check_master_state(tpcmaster_t *master) {
  if (master->slave_count == master->slave_capacity) {
    master->state = TPC_READY;
  }
}

// OUR CODE HERE
/* Copies and mallocs MSG and stores it in the SERVER->msg field so that
 * phase 2 can know what operation to do from phase 1. */
static int copy_and_store_kvmessage(tpcmaster_t *master, kvmessage_t *msg) {
  /* OUR CODE HERE: We don't need to worry about freeing mallocs, because we
     have kvmessage_free everytime at the beginning of this function. */

  (master->client_req != NULL) ? kvmessage_free(master->client_req) : free(master->client_req);
  if ((master->client_req = (kvmessage_t *) malloc(sizeof(kvmessage_t))) == NULL)
    return -1;

  if (msg->key != NULL) {
    if ((master->client_req->key = (char *) malloc(sizeof(char) * (strlen(msg->key) + 1))) == NULL)
      return -1;
    strcpy(master->client_req->key, msg->key);
  } else {
    master->client_req->key = NULL;
  }

  if (msg->value != NULL) {
    if ((master->client_req->value = (char *) malloc(sizeof(char) * (strlen(msg->value) + 1))) == NULL)
      return -1;
    strcpy(master->client_req->value, msg->value);
  } else {
    master->client_req->value = NULL;
  }

  master->client_req->type = msg->type;
  return 0;
}
