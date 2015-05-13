#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <dirent.h>
#include <stdio.h>
#include <pthread.h>
#include "kvmessage.h"
#include "kvserver.h"
#include "socket_server.h"
#include "tester.h"

#define KVSERVER_TPC_HOSTNAME "localhost"
#define KVSERVER_TPC_PORT 8162
#define KVSERVER_TPC_DIRNAME "kvserver-test"


kvserver_t my_server;
kvmessage_t reqmsg, respmsg;


int kvserver_rebuild_clean(void) {
  return kvserver_clean(&my_server);
}

int kvserver_rebuild_init(void) {
  memset(&reqmsg, 0, sizeof(kvmessage_t));
  memset(&respmsg, 0, sizeof(kvmessage_t));
  kvserver_init(&my_server, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  return 0;
}


int kvserver_rebuild_del(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);

  reqmsg.type = DELREQ;
  reqmsg.key = "key1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  /* Simulate a crash . */
  memset(&my_server, 0, sizeof(kvserver_t));
  kvserver_init(&my_server, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  kvserver_rebuild_state(&my_server);

  reqmsg.type = COMMIT;
  reqmsg.key = NULL;
  reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);
  return 1;
}

int kvserver_rebuild_abort_add(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = ABORT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  /* Simulate a crash + rebuild. */
  memset(&my_server, 0, sizeof(kvserver_t));
  kvserver_init(&my_server, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);
  kvserver_rebuild_state(&my_server);


  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, RESP);
  ASSERT_STRING_EQUAL(respmsg.message, ERRMSG_NO_KEY);

  reqmsg.type = PUTREQ;
  reqmsg.key = "key2";
  reqmsg.value = "value2";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);
  return 1;
}

int kvserver_rebuild_put_commit(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  /* Simulate a crash. */
  memset(&my_server, 0, sizeof(kvserver_t));
  kvserver_init(&my_server, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);

  /* Forcefully remove from store  */
  kvstore_del(&my_server.store, "key1");

  kvserver_rebuild_state(&my_server);

  /* Check that server completed the transaction properly */
  reqmsg.type = GETREQ;
  reqmsg.key = "key1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, GETRESP);
  ASSERT_STRING_EQUAL(respmsg.key, "key1");
  ASSERT_STRING_EQUAL(respmsg.value, "value1");
  return 1;
}

int kvserver_rebuild_clear(void) {
  reqmsg.type = PUTREQ;
  reqmsg.key = "key1";
  reqmsg.value = "value1";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);

  reqmsg.type = PUTREQ;
  reqmsg.key = "key2";
  reqmsg.value = "value2";
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, VOTE_COMMIT);

  reqmsg.type = COMMIT;
  reqmsg.key = reqmsg.value = NULL;
  kvserver_handle_tpc(&my_server, &reqmsg, &respmsg);
  ASSERT_EQUAL(respmsg.type, ACK);
	
  /* Simulate a crash. */
  memset(&my_server, 0, sizeof(kvserver_t));
  kvserver_init(&my_server, KVSERVER_TPC_DIRNAME, 4, 4, 1,
      KVSERVER_TPC_HOSTNAME, KVSERVER_TPC_PORT, true);

  kvserver_rebuild_state(&my_server);
  ASSERT_EQUAL(my_server.log.nextid, (unsigned long) 0);
  return 1;
}

test_info_t kvserver_rebuild_test[] = {
  {"kvserver rebuild from delete", kvserver_rebuild_del},
  {"kvserver rebuild from abort and then add", kvserver_rebuild_abort_add},
  {"kvserver rebuild from forceful delete", kvserver_rebuild_put_commit},
  {"kvserver rebuild and check log entry", kvserver_rebuild_clear},
  NULL_TEST_INFO
};

suite_info_t kvserver_rebuild_suite = {"KVServer Rebuild State Tests", kvserver_rebuild_init, kvserver_rebuild_clean,
  kvserver_rebuild_test};
