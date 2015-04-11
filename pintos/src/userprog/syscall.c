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
#include "filesys/filesys.h"
#include "threads/malloc.h"

static void syscall_handler (struct intr_frame *);
// OUR CODE HERE
static int wait (tid_t pid); 
static int write (int fd, const void *buffer, unsigned size);
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
struct file_wrapper *fd_to_file_wrapper (int fd);

static unsigned curr_fd;

static struct lock file_lock;
static struct lock fd_lock;

struct file_wrapper
  {
    bool closed;
    unsigned fd;
    struct file *file;
    struct list_elem thread_elem;
    // struct hash_elem hash_elem;
  };

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
  lock_init(&file_lock);
  lock_init(&fd_lock);
  curr_fd = 2;
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
      verify_args(args, 1);
      f->eax = args[1] + 1;
      break;
    }

    case SYS_WRITE: {
      verify_args(args, 3);
      f->eax = write(args[1], user_to_kernel((void *) args[2]), args[3]);
      break;
    }
    
    case SYS_HALT: {
      shutdown_power_off();
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
      verify_args(args, 2);
      f->eax = create(user_to_kernel((void *) args[1]), args[2]);
      break;
    }

    case SYS_REMOVE: {
      verify_args(args, 1);
      f->eax = remove(user_to_kernel((void *) args[1]));
      break;
    }

    case SYS_OPEN: {
      verify_args(args, 1);
      f->eax = open(user_to_kernel((void *) args[1]));
      // struct file *file = file_open (struct inode *);
      break;
    }

    case SYS_FILESIZE: {
      verify_args(args, 1);
      f->eax = filesize(args[1]);
      break;
    }

    case SYS_READ: {
      verify_args(args, 3);
      f->eax = read(args[1], user_to_kernel((void *) args[2]), args[3]);
      break;
    }

    case SYS_SEEK: {
      verify_args(args, 2);
      seek(args[1], args[2]);
      break;
    }

    case SYS_TELL: {
      verify_args(args, 1);
      // f->eax = file_tell (struct file *);
      f->eax = tell(args[1]);
      break;
    }

    case SYS_CLOSE: {
      verify_args(args, 1);
      close(args[1]);
      // file_close (struct file *);
      break;
    }
  }
}


/* Syscall handler when syscall exit is invoked. */
void exit (int status) {
  printf("%s: exit(%d)\n", thread_current()->name, status);
  struct thread *curr = thread_current();
  curr->exec_status->exit_code = status;
  // struct list_elem *e = list_begin(&curr->file_wrappers);
  // while (!list_empty(&curr->file_wrappers)) {
  //   struct file_wrapper *temp = list_entry(e, struct file_wrapper, thread_elem);
  //   e = list_remove(e);
  //   free(temp);
  // }
  struct list_elem *e;
  while (!list_empty(&curr->file_wrappers)) {
    e = list_begin(&curr->file_wrappers);
    close(list_entry(e, struct file_wrapper, thread_elem)->fd);
  }
  thread_exit();
  // README: might need to save exit code on f->eax here when user_to_kernel fails??
}

/* Method for when we want to invoke a syscall wait. */ 
static int wait (tid_t pid) {
  return process_wait(pid);
}

/* Method for when we invoke a syscall exec. */
static int exec (const char *cmd_line) {
  // const char *file_name = (char *) user_to_kernel((const char *) argv[0]);
  lock_acquire(&file_lock);
  int status = process_execute(cmd_line);
  lock_release(&file_lock);
  return status;
}

/* Syscall handler for when a syscall write is invoked. */
static int write (int fd, const void *buffer, unsigned size) {
  // const void *buffer = (void *) user_to_kernel((const void *) argv[1]);
  if (fd == STDOUT_FILENO) {
    putbuf(buffer, size);
    return size;
  } else if (fd == STDIN_FILENO) {
    exit(-1);
    return -1;
  } else {
    lock_acquire(&file_lock);
    struct file_wrapper *curr = fd_to_file_wrapper(fd);
    if (!curr) // added check
      exit(-1);
    struct file *file = curr->file;
    int bytes_written; // default for error
    /* if (file != NULL) {
      bytes_written = file_write(file, buffer, size);
    } else {
      lock_release(&file_lock);
      exit(-1); // not sure what to do here?
      return -1;
    } */
    bytes_written = file_write(file, buffer, size);
    lock_release(&file_lock);
    return bytes_written;
  }
}


/* Syscall handler for when a syscall create is invoked. */
static bool create (const char *file, unsigned initial_size) { // DONE
  // I don't think you need to lock_acquire here
  if (!file)  // added check
    exit(-1);
  lock_acquire(&file_lock);
  bool success = filesys_create(file, initial_size);
  lock_release(&file_lock);
  return success;
}

/* Syscall handler for when a syscall remove is invoked. */
static bool remove (const char *file) {
  // do I need to free the malloc here? I would need to iterate through all my children for duplicate files right? what about other threads?
  if (!file)  // added check
    exit(-1);
  lock_acquire(&file_lock);
  bool success = filesys_remove(file);
  lock_release(&file_lock);
  return success;
}

/* Syscall handler for when a syscall open is invoked. */
static int open (const char *file_) { // DONE
  // README: WHAT HAPPENS WHEN YOU TRY TO OPEN A CLOSED FILE?
  lock_acquire(&file_lock);
  struct file *file = filesys_open(file_);
  lock_release(&file_lock);
  if (!file)
    return -1;
  struct file_wrapper *f = (struct file_wrapper *) malloc(sizeof(struct file_wrapper));
  lock_acquire(&file_lock);
  f->file = file;
  f->closed = false;
  list_push_back(&thread_current()->file_wrappers, &f->thread_elem); // README: list_insert for more efficiency
  lock_release(&file_lock);
  lock_acquire(&fd_lock);
  f->fd = curr_fd++;
  lock_release(&fd_lock);
  // hash_insert(&hash_table, &f->hash_elem);
  return f->fd;
}

/* Syscall handler for when a syscall filesize is invoked. */
static int filesize (int fd) { // DONE
  lock_acquire(&file_lock);
  struct file_wrapper *curr = fd_to_file_wrapper(fd);
  if (!curr) // added check
    exit(-1);
  int size = file_length (curr->file);
  lock_release(&file_lock);
  return size;
}

/* Syscall handler for when a syscall read is invoked. */
static int read (int fd, void *buffer, unsigned length) {  // FIXME
  int size = -1;
  if (fd == STDIN_FILENO) { // i don't think i need to lock_acquire and lock_release here
    uint8_t *buffer_copy = (uint8_t *) buffer;
    unsigned i;
    for (i = 0; i < length; i++)
      buffer_copy[i] = input_getc();
    size = length;
  } else if (fd == STDOUT_FILENO) {
    // printf("I'm in read... but. what. this shouldn't be happening doe. I think?\n");
    // exit(-1);
    // return -1; // actually this should be what we're returning i think
    // exit(0);
    // size = -1; // won't hit here
    // NOTE README: this case does happen in test. Either we exit(-1) or we simply return 0...
    // exit(-1);
    size = -1; // or 0? or should I exit? idk
  } else {
    lock_acquire(&file_lock);
    struct file_wrapper *curr = fd_to_file_wrapper(fd);
    if (!curr)
      exit(-1); // added check
    size = file_read(curr->file, buffer, length);
    lock_release(&file_lock);
  }
  return size;
}

/* Syscall handler for when a syscall seek is invoked. */
static void seek (int fd, unsigned position) { // DONE
  lock_acquire(&file_lock);
  struct file_wrapper *curr = fd_to_file_wrapper(fd);
  if (!curr) // added check
    exit(-1);
  file_seek (curr->file, position);
  lock_release(&file_lock);
}

/* Syscall handler for when a syscall tell is invoked. */
static unsigned tell (int fd) { // DONE
  lock_acquire(&file_lock);
  struct file_wrapper *curr = fd_to_file_wrapper(fd);
  if (!curr) // added check
    exit(-1);
  unsigned position = file_tell(curr->file);
  lock_release(&file_lock);
  return position;
}

/* Syscall handler for when a syscall close is invoked. */
static void close (int fd) {
  if (fd == STDIN_FILENO || fd == STDOUT_FILENO) {
    exit(-1);
    // return -1; // what should I do here? or should I just exit here?
  }
  lock_acquire(&file_lock);
  struct file_wrapper *curr = fd_to_file_wrapper(fd);
  if (!curr) // added check
    exit(-1);
  if (curr->closed)
    return;
  list_remove(&curr->thread_elem);
  file_close(curr->file);
  curr->closed = true;
  free(curr);
  lock_release(&file_lock);
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
  for (i = 0; i <= argc; i++)
    verify_user_ptr((const void *) (ptr + i));
}

/* convert the user space pointer into kernel one */
void* user_to_kernel (void *ptr) {
  /* README: the line of code below is NOT run when we call verify_args()
     because this ptr is the DATA at that address, and not address itself */
  verify_user_ptr((const void *) ptr); // have to verify the pointer you're transferring is valid also.
  void* kernel_p = pagedir_get_page(thread_current()->pagedir, (const void *) ptr);
  if (!kernel_p)
    exit(-1);
  return kernel_p;
}

struct file_wrapper *
fd_to_file_wrapper (int fd_) {
  // struct file_wrapper f;
  // struct hash_elem *e;
  // f.fd = fd;
  // return hash_find (&hash_table, &f.hash_elem) == NULL ? NULL :
  //                   hash_entry(e, struct file_wrapper, hash_elem)->file;
  unsigned fd = (unsigned) fd_;
  //struct list my_files = thread_current()->file_wrappers;
  struct list_elem *e;
  for (e = list_begin(&thread_current()->file_wrappers); e != list_end(&thread_current()->file_wrappers);
       e = list_next(e)) {
    struct file_wrapper *curr = list_entry(e, struct file_wrapper, thread_elem);
    if (fd == curr->fd) {
      return curr;
    }
  }
  return NULL; // should this ever hit?
}
