#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"

static void syscall_handler (struct intr_frame *);

/* Increments the passed in integer argument by 1 */
int cmd_null (struct intr_frame *f, uint32_t *argv) {
  int i = argv[0];
  return i+1;
}

/* Writes size bytes from buffer to the open file fd */
int cmd_write (struct intr_frame *f, uint32_t *argv) {
  int fd = argv[0];
  const void *buffer = argv[1];
  unsigned size = argv[2];
  //printf("%s", buffer);
  putbuf(argv[1], argv[2]);
  return size;
}

/* Terminates the current user program, returning status to the kernel */
int cmd_exit (struct intr_frame *f, uint32_t *argv) {
  int status = argv[0];
  printf ("%s: exit(%d)\n", thread_current() -> name, status);
  f->eax = status;
  // f->error_code = status;
  thread_exit();
  return 1;
}


/* Command Lookup table */
typedef int cmd_fun_t (struct intr_frame *f, uint32_t *argv);
typedef struct fun_desc {
  cmd_fun_t *fun;
  int cmd;
} fun_desc_t;

fun_desc_t cmd_table[] = {
  // {cmd_halt, SYS_HALT, "Terminates Pintos by calling power_off()"},
  {cmd_exit, SYS_EXIT},
  // {cmd_exec, SYS_EXEC, "Runs the executable whose name is given in cmd_line"},
  // {cmd_wait, SYS_WAIT, "If process pid is still alive, waits until it dies"},
  // {cmd_create, SYS_CREATE, "Creates a new file called file initially initial_size bytes in size"},
  // {cmd_remove, SYS_REMOVE, "Deletes the file called file"},
  // {cmd_open, SYS_OPEN, "Opens the file called file"},
  // {cmd_filesize, SYS_FILESIZE, "Returns the size, in bytes, of the file open as fd"},
  // {cmd_read, SYS_READ, "Reads size bytes from the file open as fd into buffer"},
  {cmd_write, SYS_WRITE},
  // {cmd_seek, SYS_SEEK, "Changes the next byte to be read or written in open file fd to position"},
  // {cmd_tell, SYS_TELL, "Returns the position of the next byte to be read or written in open file fd"},
  // {cmd_close, SYS_CLOSE, "Closes file descriptor fd"}
  {cmd_null, SYS_NULL}
};

int lookup(int cmd) {
  int i;
  for (i=0; i < (sizeof(cmd_table)/sizeof(fun_desc_t)); i++) {
    if (cmd && cmd_table[i].cmd == cmd) return i;
  }
  return -1;
}

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
}

static void
syscall_handler (struct intr_frame *f UNUSED) 
{

  uint32_t* args = ((uint32_t*) f->esp);
  // printf("System call number: %d\n", args[0]);

  // system call table lookup
  int fundex = -1;
  int return_val;
  fundex = lookup(args[0]);
  if (fundex >= 0) {
    return_val = cmd_table[fundex].fun(f, &args[1]);
  }
  f->eax = return_val;

}