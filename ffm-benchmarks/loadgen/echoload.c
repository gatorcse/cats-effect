// Closed-loop TCP echo load generator for the FFM polling spike.
// Each connection keeps exactly one message in flight and records its round-trip time.
// usage: echoload <port> <conns> <threads> <msgsize> <warmup_s> <duration_s>
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define NBUCKETS 200000 /* 0.5us buckets up to 100ms */
#define BUCKET_NS 500

static int port, nconns, nthreads, msgsize, warmup_s, duration_s;
static volatile int phase = 0; /* 0 warmup, 1 measure, 2 stop */

typedef struct {
  int fd;
  int got;
  uint64_t sent_at;
} conn_t;

typedef struct {
  int id, n;
  conn_t *conns;
  uint64_t *hist;
  uint64_t count, overflow, max_ns;
} worker_t;

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static char *payload;

/* readiness: epoll on Linux, kqueue on macOS/BSD */
static int poller_create(void) {
#ifdef __linux__
  return epoll_create1(0);
#else
  return kqueue();
#endif
}

static void poller_add(int ep, int fd, void *ptr) {
#ifdef __linux__
  struct epoll_event ev = {.events = EPOLLIN, .data.ptr = ptr};
  epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev);
#else
  struct kevent ev;
  EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, ptr);
  kevent(ep, &ev, 1, NULL, 0, NULL);
#endif
}

/* waits up to 100ms; fills ready[] with the registered pointers */
static int poller_wait(int ep, void **ready, int max) {
#ifdef __linux__
  struct epoll_event evs[256];
  int k = epoll_wait(ep, evs, max, 100);
  for (int j = 0; j < k; j++) ready[j] = evs[j].data.ptr;
#else
  struct kevent evs[256];
  struct timespec to = {0, 100000000};
  int k = kevent(ep, NULL, 0, evs, max, &to);
  for (int j = 0; j < k; j++) ready[j] = evs[j].udata;
#endif
  return k;
}

static void send_msg(conn_t *c) {
  int off = 0;
  c->sent_at = now_ns();
  while (off < msgsize) {
    ssize_t w = write(c->fd, payload + off, msgsize - off);
    if (w > 0) off += w;
    else if (w < 0 && errno != EAGAIN && errno != EINTR) { perror("write"); exit(1); }
  }
  c->got = 0;
}

static void *run(void *arg) {
  worker_t *w = arg;
  int ep = poller_create();
  char *buf = malloc(65536);
  struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
  inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
  for (int i = 0; i < w->n; i++) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) != 0) { perror("connect"); exit(1); }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    w->conns[i].fd = fd;
    poller_add(ep, fd, &w->conns[i]);
  }
  for (int i = 0; i < w->n; i++) send_msg(&w->conns[i]);
  void *ready[256];
  while (phase != 2) {
    int k = poller_wait(ep, ready, 256);
    for (int j = 0; j < k; j++) {
      conn_t *c = ready[j];
      ssize_t r = read(c->fd, buf, 65536);
      if (r <= 0) {
        if (r < 0 && (errno == EAGAIN || errno == EINTR)) continue;
        fprintf(stderr, "conn closed\n");
        exit(1);
      }
      c->got += r;
      if (c->got >= msgsize) {
        uint64_t t = now_ns(), lat = t - c->sent_at;
        if (phase == 1) {
          w->count++;
          uint64_t b = lat / BUCKET_NS;
          if (b >= NBUCKETS) w->overflow++; else w->hist[b]++;
          if (lat > w->max_ns) w->max_ns = lat;
        }
        send_msg(c);
      }
    }
  }
  return NULL;
}

static double pct(uint64_t *hist, uint64_t total, double p) {
  uint64_t target = (uint64_t)(p * total), acc = 0;
  for (int i = 0; i < NBUCKETS; i++) {
    acc += hist[i];
    if (acc >= target && acc > 0) return (i + 0.5) * BUCKET_NS / 1000.0;
  }
  return NBUCKETS * BUCKET_NS / 1000.0;
}

int main(int argc, char **argv) {
  if (argc < 7) { fprintf(stderr, "usage: echoload port conns threads msgsize warmup_s duration_s\n"); return 2; }
  port = atoi(argv[1]); nconns = atoi(argv[2]); nthreads = atoi(argv[3]);
  msgsize = atoi(argv[4]); warmup_s = atoi(argv[5]); duration_s = atoi(argv[6]);
  payload = malloc(msgsize);
  memset(payload, 'x', msgsize);
  worker_t *ws = calloc(nthreads, sizeof(worker_t));
  pthread_t *ts = calloc(nthreads, sizeof(pthread_t));
  for (int i = 0; i < nthreads; i++) {
    ws[i].id = i;
    ws[i].n = nconns / nthreads + (i < nconns % nthreads ? 1 : 0);
    ws[i].conns = calloc(ws[i].n, sizeof(conn_t));
    ws[i].hist = calloc(NBUCKETS, sizeof(uint64_t));
    pthread_create(&ts[i], NULL, run, &ws[i]);
  }
  sleep(warmup_s);
  fprintf(stderr, "MEASURE_START %llu\n", (unsigned long long)(time(NULL)));
  uint64_t t0 = now_ns();
  phase = 1;
  sleep(duration_s);
  phase = 2;
  uint64_t t1 = now_ns();
  fprintf(stderr, "MEASURE_END %llu\n", (unsigned long long)(time(NULL)));
  for (int i = 0; i < nthreads; i++) pthread_join(ts[i], NULL);
  uint64_t *hist = calloc(NBUCKETS, sizeof(uint64_t));
  uint64_t total = 0, over = 0, mx = 0;
  for (int i = 0; i < nthreads; i++) {
    total += ws[i].count; over += ws[i].overflow;
    if (ws[i].max_ns > mx) mx = ws[i].max_ns;
    for (int b = 0; b < NBUCKETS; b++) hist[b] += ws[i].hist[b];
  }
  double secs = (t1 - t0) / 1e9;
  printf("{\"msgs\":%llu,\"secs\":%.3f,\"rps\":%.0f,\"mbps\":%.1f,\"p50_us\":%.1f,\"p90_us\":%.1f,\"p99_us\":%.1f,\"p999_us\":%.1f,\"max_us\":%.1f,\"overflow\":%llu}\n",
         (unsigned long long)total, secs, total / secs, total * (double)msgsize * 2 / secs / 1e6,
         pct(hist, total, .5), pct(hist, total, .9), pct(hist, total, .99), pct(hist, total, .999),
         mx / 1000.0, (unsigned long long)over);
  return 0;
}
