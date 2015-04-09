#include "userprog/process.h"
#include "userprog/syscall.h"
#include "userprog/pagedir.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "devices/shutdown.h"

static void syscall_handler (struct intr_frame *);
// OUR CODE HERE
static void halt (void);
static int wait (tid_t pid); 
static int write (int fd, const void *buffer, unsigned size);
static void exit (int status);
static int exec (const char *cmd_line); 
void verify_user_ptr (const void* ptr);
void verify_args(uint32_t *ptr, int argc);
void* user_to_kernel (void *vaddr);

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
  verify_user_ptr(args);
  switch (args[0]) {
    
    case SYS_EXIT: {
      verify_args(args, 1);
      f->eax = args[1];
      exit(args[1]);
      break;
    }

    case SYS_NULL: {
      f->eax = args[1] + 1;
      break;
    }

    case SYS_WRITE: {
      verify_args(args, 3);
      f->eax = write(args[1], user_to_kernel((void *) args[2]), args[3]);
      break;
    }
    
    case SYS_HALT: {
      halt();
      break;
    }

    case SYS_WAIT: {
      verify_args(args, 1);
      f->eax = wait((tid_t) args[1]);
      break;
    }

    case SYS_EXEC: {
      verify_args(args, 1);
      f->eax = exec(user_to_kernel((void *) args[1]));
      break;
    }
  }
}


/* Syscall handler when syscall exit is invoked. */
static void exit (int status) {
  printf("%s: exit(%d)\n", thread_current()->name, status);
  thread_current()->exec_status->exit_code = status;
  thread_exit();
  // README: might need to save exit code on f->eax here when user_to_kernel fails??
}

/* Method for when syscall halt is invoked. */
static void halt() {
  shutdown_power_off();
}

/* Method for when we want to invoke a syscall wait. */ 
static int wait (tid_t pid) {
  return process_wait(pid);
}

/* Method for when we invoke a syscall exec. */
static int exec (const char *cmd_line) {
  // const char *file_name = (char *) user_to_kernel((const char *) argv[0]);
  return process_execute(cmd_line);
}

/* Syscall handler for when a syscall write is invoked. */
static int write (int fd UNUSED, const void *buffer, unsigned size) {
  // const void *buffer = (void *) user_to_kernel((const void *) argv[1]); 
  putbuf(buffer, size);
  return size;
}

/* Checks to see if the ptr is valid or not. A pointer is valid if and only if
   it is not a null pointer, if it points to a mapped portion of virtual memory,
   and it lies below PHYS_BASE.

   If @ptr is invalid, we syscall exit with error code.  */
void verify_user_ptr (const void* ptr) {
  if (ptr == NULL || !is_user_vaddr(ptr) || pagedir_get_page(thread_current()->pagedir, ptr) == NULL)
    exit(-1);
}

/* Checks if all arguments, of length ARGC, have a valid address starting
   from PTR + 1. */
void verify_args(uint32_t *ptr, int argc) {
  int i;
  for (i = 1; i <= argc; i++)
    verify_user_ptr((const void *) (ptr + i));
}

/* convert the user space pointer into kernel one */
void* user_to_kernel (void *ptr) {
  /* README: the line of code below is NOT run when we call verify_args()
     because this ptr is the DATA at that address, and not address itself */
  verify_user_ptr((const void *) ptr); // have to verify the pointer you're transferring is valid also.
  void* kernel_p = pagedir_get_page(thread_current()->pagedir, (const void *) ptr); 
  if (kernel_p == NULL)
    exit(-1);
  return kernel_p;
}

