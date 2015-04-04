#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"

static void syscall_handler (struct intr_frame *);
static void syscall_null (struct intr_frame *f, uint32_t *argv);
static void syscall_write (struct intr_frame *f, uint32_t *argv);
static void syscall_exit (struct intr_frame *f, uint32_t *argv);

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
      syscall_exit(f, &args[1]);
      break;
    }

    case SYS_NULL: {
      syscall_null(f, &args[1]);
      break;
    }

    case SYS_WRITE: {
      syscall_write(f, &args[1]);
      break;
    }
    /*
    case: SYS_HALT {

    }

    case: SYS_WAIT {

    }

    case: SYS_CREATE {

    } */
  }
}

/* Syscall handler when syscall exit is invoked.
 *
 * First sets the eax register of the intr_frame and then thread exits. */
static void syscall_exit (struct intr_frame *f, uint32_t *argv) {
  int status = argv[0];
  printf("%s: exit(%d)\n", thread_current()->name, status);
  f->eax = status;
  thread_exit();
}

/* Syscall handler when syscall null is invoked. Dummy syscall that
   typicall is implemented in most operating systems, this is just a warm
   up for our pintos project. */
static void syscall_null (struct intr_frame *f, uint32_t *argv) {
  f->eax = argv[0] + 1;
}

/* Syscall handler when syscall write is invoked. */
static void syscall_write (struct intr_frame *f, uint32_t *argv) {
  // int fd = argv[0];
  const void *buffer = (const void *) argv[1];
  unsigned size = argv[2];
  putbuf(buffer, size);
  f->eax = size;
}

