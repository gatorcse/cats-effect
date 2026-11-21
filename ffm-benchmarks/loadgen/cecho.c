// Reference "speed of light" echo server: one epoll loop per thread, SO_REUSEPORT, level-triggered.
// usage: cecho <port> <threads>
#define _GNU_SOURCE
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

static int port;

static void *loop(void *arg) {
  (void)arg;
  int ls = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0), one = 1;
  setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  setsockopt(ls, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
  struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
  if (bind(ls, (struct sockaddr *)&a, sizeof a) || listen(ls, 4096)) { perror("bind/listen"); exit(1); }
  int ep = epoll_create1(0);
  struct epoll_event ev = {.events = EPOLLIN, .data.fd = ls}, evs[256];
  epoll_ctl(ep, EPOLL_CTL_ADD, ls, &ev);
  char buf[16384];
  for (;;) {
    int n = epoll_wait(ep, evs, 256, -1);
    for (int i = 0; i < n; i++) {
      int fd = evs[i].data.fd;
      if (fd == ls) {
        int c;
        while ((c = accept4(ls, NULL, NULL, SOCK_NONBLOCK)) >= 0) {
          setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
          struct epoll_event ce = {.events = EPOLLIN, .data.fd = c};
          epoll_ctl(ep, EPOLL_CTL_ADD, c, &ce);
        }
        continue;
      }
      ssize_t r = read(fd, buf, sizeof buf);
      if (r <= 0) {
        if (r < 0 && errno == EAGAIN) continue;
        close(fd);
        continue;
      }
      ssize_t off = 0;
      while (off < r) {
        ssize_t w = write(fd, buf + off, r - off);
        if (w > 0) off += w; else if (errno != EAGAIN) break;
      }
    }
  }
  return NULL;
}

int main(int argc, char **argv) {
  port = atoi(argv[1]);
  int t = argc > 2 ? atoi(argv[2]) : 1;
  pthread_t th[64];
  for (int i = 0; i < t; i++) pthread_create(&th[i], NULL, loop, NULL);
  printf("cecho port=%d threads=%d\n", port, t);
  fflush(stdout);
  pthread_join(th[0], NULL);
}
