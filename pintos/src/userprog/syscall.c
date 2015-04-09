#include "userprog/process.h"
#include "userprog/syscall.h"
#include "userprog/pagedir.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "devices/shutdown.h"
// OUR CODE HERE
#include "filesys/file.h"
#include "devices/input.h"

static struct lock file_lock;
static struct hash hash_table;


static void syscall_handler (struct intr_frame *);
// OUR CODE HERE
static void halt (void);
static int wait (tid_t pid); 
static int write (int fd, const void *buffer, unsigned size);
static void exit (int status);
static int exec (const char *cmd_line);
static bool create (const char *file, unsigned initial_size);
static bool remove (const char *file);
static int open (const char *file);
static int filesize (int fd);
static int read (int fd, void *buffer, unsigned length);
static void seek (int fd, unsigned position);
static unsigned tell (int fd);
static void close (int fd); 
void verify_user_ptr (const void* ptr);
void verify_args(uint32_t *ptr, int argc);
void* user_to_kernel (void *vaddr);

static unsigned curr_fd;

struct file
  {
    unsigned fd;
    struct hash_elem elem;
  };

/* Returns a hash value for file f. */
unsigned hash_func () 
{
  return curr_fd++;
}

/* Returns true if file a preceds file b. */
bool
file_less (struct hash_elem *a_, struct hash_elem *b_) 
{
  struct file *a = hash_entry (a_, struct file, hash_elem);
  struct file *b = hash_entry (b_, struct file, hash_elem);
  return a->fd < b->fd;
}

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
  lock_init(&file_lock);
  hash_init(&hash_table, &hash_func, &less_func, NULL);
}

// enum 
//   {
//     SYS_HALT,                   /* Halt the operating system. */
//     SYS_EXIT,                   /* Terminate this process. */
//     SYS_EXEC,                   /* Start another process. */
//     SYS_WAIT,                   /* Wait for a child process to die. */
//     SYS_CREATE,                 /* Create a file. */
//     SYS_REMOVE,                 /* Delete a file. */
//     SYS_OPEN,                   /* Open a file. */
//     SYS_FILESIZE,               /* Obtain a file's size. */
//     SYS_READ,                   /* Read from a file. */
//     SYS_WRITE,                  /* Write to a file. */
//     SYS_SEEK,                   /* Change position in a file. */
//     SYS_TELL,                   /* Report current position in a file. */
//     SYS_CLOSE,                  /* Close a file. */
//     SYS_NULL,                   /* Returns arg incremented by 1 */

//   };


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
      lock_acquire(&file_lock);
      f->eax = write(args[1], user_to_kernel((void *) args[2]), args[3]);
      lock_release(&file_lock);
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

    case SYS_CREATE: {
      f->eax = create((const char *)args[1], (unsigned) args[2])
      break;
    }

    case SYS_REMOVE: {
      f->eax = remove((const char *)args[1])
      break;
    }

    case SYS_OPEN: {
      f->eax = open((const char *) args[1])
      struct file *file = file_open (struct inode *);
      break;
    }

    case SYS_FILESIZE: {
      f->eax = filesize((int) args[1]);
      break;
    }

    case SYS_READ: {
      f->eax = read((int) fd, (void *) buffer, (unsigned) length);
      break;
    }

    case SYS_SEEK: {
      seek((int) args[1], (unsigned) args[2])
      break;
    }

    case SYS_TELL: {
      f->eax = file_tell (struct file *);
      break;
    }

    case SYS_CLOSE: {
      close((int) args[1])
      file_close (struct file *);
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
static int write (int fd, const void *buffer, unsigned size) {
  // const void *buffer = (void *) user_to_kernel((const void *) argv[1]); 
  if (fd == STDOUT_FILENO)
    putbuf(buffer, size);
    return size;
  // else if (fd == STDIN_FILENO)
  //   error
  else {
    struct file *file;
    return (int) file_write(file, buffer, (off_t) size);
  }
  
}

/* Syscall handler for when a syscall create is invoked. */
static bool create (const char *file, unsigned initial_size) {
  block_sector_t sector;
  return inode_create(sector, (off_t) initial_size); 
}

/* Syscall handler for when a syscall remove is invoked. */
static bool remove (const char *file) {
  struct inode *inode;
  inode_remove(inode);
}

/* Syscall handler for when a syscall open is invoked. */
static int open (const char *file) {
  struct inode *inode;
  struct file *f = file_open (inode);
  int fd;
  return fd;
}
/* Syscall handler for when a syscall filesize is invoked. */
static int filesize (int fd) {
  struct file *file;
  return (int) file_length (file);
}

/* Syscall handler for when a syscall read is invoked. */
static int read (int fd, void *buffer, unsigned length) {
  if (fd == STDIN_FILENO)
    return (int) input_getc();
  // else if (fd == STDOUT_FILENO)
  //   error
  else {
    struct file *file;
    return (int) file_read(file, buffer, (off_t) length);
  }
}

/* Syscall handler for when a syscall seek is invoked. */
static void seek (int fd, unsigned position) {
  struct file *file;
  file_seek (file, (off_t) position);
}

/* Syscall handler for when a syscall tell is invoked. */
static unsigned tell (int fd) {
  struct file *file;
  return (unsigned) file_tell(file);
}

/* Syscall handler for when a syscall close is invoked. */
static void close (int fd) {
  struct file *file;
  file_close(file);
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

