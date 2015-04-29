#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "kvserver.h"
#include "kvconstants.h"
#include "socket_server.h"
#include "wq.h"

#define TIMEOUT 100

static void* server_run_helper(void *arg);

/* Struct that will be passed in to the server_run_helper thread function,
   which itself can only take one argument as parameter. */
struct server_helper {
  /* All fields are variables found in server_run() that are necessary to pass into
     server_run_helper() to listen and accept client requests with the server in the
     helper thread function. */
  server_t *server;
  int sock_fd;
  struct sockaddr_in client_address;  
  size_t client_address_length;
};

/* Handles requests under the assumption that SERVER is a TPC Master. */
void handle_master(server_t *server) {
  int sockfd;
  tpcmaster_t *tpcmaster = &server->tpcmaster;
  sockfd = (intptr_t) wq_pop(&server->wq);
  tpcmaster->handle(tpcmaster, sockfd, NULL);
}

/* Handles requests under the assumption that SERVER is a kvserver slave. */
void handle_slave(server_t *server) {
  int sockfd;
  kvserver_t *kvserver = &server->kvserver;
  sockfd = (intptr_t) wq_pop(&server->wq);
  kvserver->handle(kvserver, sockfd, NULL);
}

/* Handles requests for _SERVER. */
void *handle(void *_server) {
  server_t *server = (server_t *) _server;
  if (server->master) {
    handle_master(server);
  } else {
    handle_slave(server);
  }
  return NULL;
}

/* Connects to the host given at HOST:PORT using a TIMEOUT second timeout.
 * Returns a socket fd which should be closed, else -1 if unsuccessful. */
int connect_to(const char *host, int port, int timeout) {
  struct sockaddr_in addr;
  struct hostent *ent;
  int sockfd;

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  ent = gethostbyname(host);
  if (ent == NULL) {
    return -1;
  }
  bzero((char *) &addr, sizeof(addr));
  addr.sin_family = AF_INET;
  bcopy((char *)ent->h_addr, (char *)&addr.sin_addr.s_addr, ent->h_length);
  addr.sin_port = htons(port);
  if (timeout > 0) {
    struct timeval t;
    t.tv_sec = timeout;
    t.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *) &t, sizeof(t));
  }
  if (connect(sockfd,(struct sockaddr *) &addr, sizeof(addr)) < 0) {
    return -1;
  }
  return sockfd;
}

/* Runs SERVER such that it indefinitely (until server_stop is called) listens
 * for incoming requests at HOSTNAME:PORT. If CALLBACK is not NULL, makes a
 * call to CALLBACK with NULL as its parameter once SERVER is actively
 * listening for requests (this is for testing purposes).
 *
 * As given, this function will synchronously handle only a single request
 * at a time. It is your task to modify it such that it can handle up to
 * SERVER->max_threads jobs at a time asynchronously. */
int server_run(const char *hostname, int port, server_t *server,
               callback_t callback) {
  int sock_fd, socket_option;
  struct sockaddr_in client_address;
  size_t client_address_length = sizeof(client_address);
  wq_init(&server->wq);
  server->listening = 1;
  server->port = port;
  server->hostname = (char *) malloc(strlen(hostname) + 1);
  strcpy(server->hostname, hostname);

  sock_fd = socket(PF_INET, SOCK_STREAM, 0);
  server->sockfd = sock_fd;
  if (sock_fd == -1) {
    fprintf(stderr, "Failed to create a new socket: error %d: %s\n", errno,
        strerror(errno));
    exit(errno);
  }
  socket_option = 1;
  if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &socket_option,
      sizeof(socket_option)) == -1) {
    fprintf(stderr, "Failed to set socket options: error %d: %s\n", errno,
        strerror(errno));
    exit(errno);
  }
  memset(&client_address, 0, sizeof(client_address));
  client_address.sin_family = AF_INET;
  client_address.sin_addr.s_addr = INADDR_ANY;
  client_address.sin_port = htons(port);

  if (bind(sock_fd, (struct sockaddr *) &client_address,
      sizeof(client_address)) == -1) {
    fprintf(stderr, "Failed to bind on socket: error %d: %s\n", errno, strerror(errno));
    exit(errno);
  }

  if (listen(sock_fd, 1024) == -1) {
    fprintf(stderr, "Failed to listen on socket: error %d: %s\n", errno,
        strerror(errno));
    exit(errno);
  }

  if (callback != NULL){
    callback(NULL);
  }

  // OUR CODE HERE: initializing argument struct for helper thread function for pool thread.
  struct server_helper *server_helper_ptr =
      (struct server_helper *) malloc(sizeof(struct server_helper));
  server_helper_ptr->server = server;
  server_helper_ptr->sock_fd = sock_fd;
  server_helper_ptr->client_address = client_address;
  server_helper_ptr->client_address_length = client_address_length;

  pthread_t *threads = (pthread_t *) malloc(server->max_threads * sizeof(pthread_t));
  int i;
  for (i = 0; i < server->max_threads; i++) {
    pthread_create(&threads[i], NULL, server_run_helper, server_helper_ptr);
  }
  // wait for all threads to terminate, then we can free our server_helper_ptr
  for (i = 0; i < server->max_threads; i++) {
    pthread_join(threads[i], NULL);
  }
  free(server_helper_ptr);

  shutdown(sock_fd, SHUT_RDWR);
  close(sock_fd);
  return 0;
}

/* Helper function to allow a thread to serve client requests on the server
   found in server_run. All necessary arguments are found in the server_helper struct. */
static void* server_run_helper(void *arg_) {
  struct server_helper *arg = (struct server_helper *) arg_;
  while (arg->server->listening) {
    int client_sock = accept(arg->sock_fd, (struct sockaddr *) &arg->client_address,
        (socklen_t *) &arg->client_address_length);
    if (client_sock > 0) {
      wq_push(&arg->server->wq, (void *) (intptr_t) client_sock);
      handle(arg->server);
    }
  }
  pthread_exit(0);
}

/* Stops SERVER from continuing to listen for incoming requests. */
void server_stop(server_t *server) {
  server->listening = 0;
  shutdown(server->sockfd, SHUT_RDWR);
  close(server->sockfd);
}
