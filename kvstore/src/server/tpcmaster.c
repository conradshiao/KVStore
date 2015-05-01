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

static int port_cmp(tpcslave_t *a, tpcslave_t *b);

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

  char *port = reqmsg->key;
  char *hostname = reqmsg->value;
  int port_strlen = strlen(reqmsg->key);
  int hostname_strlen = strlen(reqmsg->value);

  char *format_string = (char *) malloc((port_strlen + 1 + hostname_strlen + 1) * sizeof(char));
  if (format_string == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }

  strcpy(format_string, port);
  strcat(format_string, ":");
  strcat(format_string, hostname);
  int64_t hash_val = hash_64_bit(format_string);
  free(format_string);
  /* Check to see if slave is still in the list. */
  tpcslave_t *elt;
  DL_FOREACH(master->slaves_head, elt) {
    if (elt->id > hash_val)
      break;
    if (elt->id == hash_val) { // slave already in list
      respmsg->message = MSG_SUCCESS;
      return;
    }
  }

  if (slave_count == slave_capacity) { // is this an error?
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  } else {
    master->slave_count++;
  }

  tpcslave_t *slave = (tpcslave_t *) malloc(sizeof(tpcslave_t));
  if (slave == NULL) {
    respmsg->message = ERRMSG_GENERIC_ERROR;
    return;
  }

  /* Filling in appropriate fields of tpcslave_t */
  slave->id = hash_val;
  slave->host = (char *) malloc((hostname_strlen + 1) * sizeof(char));
  strcpy(slave->host, hostname);
  char *ptr;
  slave->port = strtol(port, &ptr, 10);

  //FIXME: where do i set the kvserver_t struct to stuff into the slave?
  // DL_SORT(master->slaves_head, port_cmp);
}

/* Comparator function to be used to sort our DL list of tpcslave_t slaves. */
static int port_cmp(tpcslave_t *a, tpcslave_t *b) {
  return a->id < b->id;
}

/* Hashes KEY and finds the first slave that should contain it.
 * It should return the first slave whose ID is greater than the
 * KEY's hash, and the one with lowest ID if none matches the
 * requirement.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_primary(tpcmaster_t *master, char *key) {
  // OUR CODE HERE: if the list of slaves are sorted by id
  int64_t hash_val = hash_64_bit(key);
  tpslave_t *elt;
  DL_FOREACH(master->slaves_head, elt)
    {
      if (elt->id > hash_val)
        return elt;
    }
  return master->slaves_head;
}

/* Returns the slave whose ID comes after PREDECESSOR's, sorted
 * in increasing order.
 *
 * Checkpoint 2 only. */
tpcslave_t *tpcmaster_get_successor(tpcmaster_t *master, tpcslave_t *predecessor) {
  // OUR CODE HERE: if the list of slaves are sorted by id
  tpslave_t *elt;
  bool saw_successor = false;
  DL_FOREACH(master->slaves_head, elt)
    {
      if (saw_successor)
        return elt;
      if (elt == predecessor)
        saw_successor = true;
    }
  return master->slaves_head;
  // i'm assuming we're guaranteed that predecessor is always in our list...
}

/* Handles an incoming GET request REQMSG, and populates the appropriate fields
 * of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
 * kvmessage_t structs.
 *
 * Checkpoint 2 only. */
void tpcmaster_handle_get(tpcmaster_t *master, kvmessage_t *reqmsg,
    kvmessage_t *respmsg) {
  if (respmsg == NULL || reqmsg == NULL)
    return;
  // respmsg->message = ERRMSG_NOT_IMPLEMENTED;
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
  // respmsg->message = ERRMSG_NOT_IMPLEMENTED;
  if (respmsg == NULL || reqmsg == NULL)
    return;

  int error;
  switch (reqmsg->type) {

    case REGISTER:
      break;

    case VOTE_COMMIT:
      break;

    case VOTE_ABORT:
      break;

    case COMMIT:
      break;

    case ABORT:
      break;

    case ACK:
      break;

    default:
      break;
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
  // respmsg->message = ERRMSG_NOT_IMPLEMENTED;
  if (respmsg == NULL || reqmsg == NULL)
    return;

  tpcslave_t *elt;
  time_t ltime = time(NULL);
  printf("%s\n", asctime(location(&ltime)));
  printf("Slaves:\n");
  DL_FOREACH(master->slaves_head, elt)
    {
      kvserver_handle_no_tpc(elt->kvserver, reqmsg, respmsg); // is it no_tpc or tpc?
      delay(2000); // 2 second wait
      /*
      if (response)
        print out
      */
        printf("{%s, %d}\n", elt->kvserver->hostname, elt->kvserver->port);
    }
  dl_foreach_safe
  tpcslave_t *curr_slave = slaves_head;


  char info[1024], buf[256];
  time_t ltime = time(NULL);
  strcpy(info, asctime(localtime(&ltime)));
  sprintf(buf, "{%s, %d}", server->hostname, server->port);
  strcat(info, buf);
  char *msg = malloc(strlen(info));
  strcpy(msg, info);
  return msg;
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
