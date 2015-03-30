#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"

struct lock file_lock;

static void syscall_handler (struct intr_frame *);
int syscall_null (int i);
int syscall_write (int fd, const void *buffer, unsigned size);
void syscall_exit (int status);
static int fd_to_file(int fd);

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
  // OUR CODE HERE:
  lock_init(&file_lock);
}

static void
syscall_handler (struct intr_frame *f) 
{
  uint32_t* args = ((uint32_t*) f->esp);
  // printf("System call number: %d\n", args[0]);
  switch (args[0]) {

  case SYS_EXIT:
    // f->eax = args[1];
    // printf("exit code: %d\n", args[1]);
    // thread_exit();
    f->eax = args[1];
    thread_exit();
    break;
  

  case SYS_NULL:
    f->eax = args[1] + 1;
    // f->eax = syscall_null(args[1]);
    break;
  

  case SYS_WRITE:
    f->eax = syscall_write((int) args[1], (const void *) args[2], (unsigned) args[3]);
  
  /*
  case: SYS_HALT {

  }

  case: SYS_WAIT {

  }

  case: SYS_CREATE {

  } */
}


int syscall_write(int fd, const void *buffer, unsigned size) {
  // if (fd == STDOUT_FILENO) {
  //   putbuf((const char *) buffer, size);
  //   return size;
  // } else {
  //   lock_acquire(&file_lock);
  //   int ans;
  //   struct file *file = fd_to_file(fd);
  //   if (file != NULL) {
  //     ans = file_write(file, buffer, size);
  //   } else {
  //     ans = ERROR;
  //   }
  //   lock_release(&file_lock);
  //   return ans;
  // }

  // dummy
  return printf("%s\n", (char*) buffer);
}


// int fd_to_file(int fd) {

// }


// /*
// Terminates the current user program, returning status to the kernel.
// */
// void syscall_exit (int status) 
// {

// }

