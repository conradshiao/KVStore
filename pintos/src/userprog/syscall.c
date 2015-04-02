#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"

static void syscall_handler (struct intr_frame *);

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
}

static void
syscall_handler (struct intr_frame *f) 
{
  uint32_t* args = ((uint32_t*) f->esp);
  // OUR CODE HERE
  switch (args[0]) {
    
    case SYS_EXIT: {
      int status = args[1];
      struct thread *cur = thread_current();
      printf ("%s: exit(%d)\n", cur -> name, status);
      // if (cur->wait_status->ref_cnt < 2) {
      //   cur->wait_status->exit_code = status;
      // }
      f->eax = status;
      thread_exit();
      break;
    }

    case SYS_NULL: {
      int i = args[1];
      f->eax = i + 1;
      break;
    }

    case SYS_WRITE: {
      int fd = args[1];
      const void *buffer = args[2];
      unsigned size = args[3];
      if (fd == STDOUT_FILENO)
        putbuf(buffer, size);
      f->eax = size;
      break;
    }
    case SYS_HALT: {
      break;
    }
    case SYS_WAIT: {
      // tid_t tid = args[1];
      // f->eax = process_wait(tid);
      break;
    }
    case SYS_EXEC: {
      const char *file_name = args[1];
      f->eax = process_execute(file_name);
      break;
    } 
  }
}
