#include "util/perfsvg.h"


#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>

namespace kv {
#ifndef __APPLE__
static pid_t __perf_pid = 0;
#endif

void 
debug_perf_ppid(void)
{
  #ifndef __APPLE__
  const pid_t ppid = getppid();
  char tmp[1024];
  sprintf(tmp, "/proc/%d/cmdline", ppid);
  FILE * const fc = fopen(tmp, "r");
  const size_t nr = fread(tmp, 1, 1020, fc); 
  fclose(fc);
  // look for "perf record"
  if (nr < 11) return;
  tmp[nr] = '\0';
  for (size_t i = 0; i < nr; i++) {
    if (tmp[i] == 0) tmp[i] = ' '; 
  }
  char * const perf = strstr(tmp, "perf record");
  if (perf == NULL) return;
  // it should be
  __perf_pid = ppid;
  #endif
}

  void
debug_perf_switch(void)
{
  #ifndef __APPLE__
  if (__perf_pid > 0) kill(__perf_pid, SIGUSR2);
  #endif
}

  void
debug_perf_stop(void)
{
  #ifndef __APPLE__
  if (__perf_pid > 0) kill(__perf_pid, SIGINT);
  #endif
}

}