#include "userprog/process.h"
#include "userprog/syscall.h"
#include "userprog/pagedir.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "devices/shutdown.h"

#define USER_VADDR_START 0x08048000

static void syscall_handler (struct intr_frame *);
// static void syscall_null (struct intr_frame *f, uint32_t *argv);
static int syscall_wait (tid_t pid); 
static int syscall_write (int fd, const void *buffer, unsigned size);
static void syscall_exit (int status);
static int syscall_exec (const char *cmd_line); 
void verify_user_ptr (const void* ptr);
void verify_arg(const void* ptr, int argc);
void * user_to_kernel (const void *vaddr);

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
      verify_arg(args, 1);
      f->eax = args[1];
      syscall_exit(args[1]);
      break;
    }

    case SYS_NULL: {
      f->eax = args[1] + 1;
      break;
    }

    case SYS_WRITE: {
      verify_arg(args, 3);
      f->eax = syscall_write(args[1], user_to_kernel(args[2]), args[3]);
      break;
    }
    
    case SYS_HALT: {
      shutdown_power_off();
      break;
    }

    case SYS_WAIT: {
      verify_arg(args, 1);
      f->eax = syscall_wait(args[1]);
      break;
    }

    case SYS_EXEC: {
      verify_arg(args, 1);
      f->eax = syscall_exec(user_to_kernel(args[1]));
      break;
    }
  }
}

// void exit_on_invalid_ptr(const void* vaddr) {
//   if (!is_user_vaddr(vaddr) || vaddr == NULL || /* Invalid memory segment. */) { // that is how you check for null pointers right?
//     // README: terminate processes and free resources here if invalid pointer
//   }
// }

/* Syscall handler when syscall exit is invoked.
 *
 * First sets the eax register of the intr_frame and then thread exits. */
// static void syscall_exit (struct intr_frame *f, uint32_t *argv) {
static void syscall_exit (int status) {
  // int status = argv[0];
  printf("%s: exit(%d)\n", thread_current()->name, status);
  // f->eax = status;
  thread_current()->exec_status->exit_code = status;
  thread_exit();
}


static int syscall_wait (tid_t pid) {
  return (int) process_wait(pid);
}

// static void syscall_exec (struct intr_frame *f, uint32_t *argv) {
static int syscall_exec (const char *cmd_line) {
  // const char *file_name = (char *) user_to_kernel((const char *) argv[0]);
  return (int) process_execute(cmd_line);
}

/* Syscall handler when syscall write is invoked. */
static int syscall_write (int fd, const void *buffer, unsigned size) {
  // int fd = argv[0];
  // const void *buffer = (void *) user_to_kernel((const void *) argv[1]);
  
  putbuf(buffer, size);
  // f->eax = size;
  return (int) size;
}

/* check if the ptr is valid or not  */
void verify_user_ptr (const void* ptr) {
  if (!is_user_vaddr(ptr) || ptr < USER_VADDR_START) {
    syscall_exit(-1);
  }
}

/*check if all fields in args have a valid address*/
void verify_arg(const void* ptr, int argc) {
  int i;
  for (i = 1; i <= argc; i++)
  {
    verify_user_ptr(ptr+i);
  }
}

/* convert the user space pointer into kernel one */
void* user_to_kernel (const void* ptr) {
  verify_user_ptr(ptr);
  struct thread* current = thread_current();
  void* kernel_p = pagedir_get_page(current->pagedir, ptr); 
  if (!kernel_p) {
    syscall_exit(-1);
  }
  return kernel_p;
}
