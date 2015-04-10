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
static struct lock fd_lock;
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
// static unsigned hash_func(struct hash_elem *f_, void *aux);
// static bool file_less (struct hash_elem *a_, struct hash_elem *b_);

// static unsigned curr_fd;

// struct file_wrapper
//   {
//     unsigned fd;
//     struct list_elem thread_elem
//     struct hash_elem hash_elem;
//     struct file *file;
//   };


/* Returns a hash value for file f. */
static struct file * 
hash_func (struct hash_elem *f_, void *aux UNUSED)
{
  struct file_wrapper *f = hash_entry (f_, struct page_wrapper, hash_elem);
  return f->file;
}

 Returns true if file a precedes file b. 
static bool
hash_less (struct hash_elem *a_, struct hash_elem *b_) 
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
  // lock_init(&fd_lock);
  // hash_init(&hash_table, &hash_func, &hash_less, NULL);
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
      verify_args(args, 1);
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
      verify_args(args, 2);
      f->eax = create((const char *)args[1], (unsigned) args[2]);
      break;
    }

    case SYS_REMOVE: {
      verify_args(args, 1);
      f->eax = remove((const char *)args[1]);
      break;
    }

    case SYS_OPEN: {
      verify_args(1);
      f->eax = open((const char *) args[1]);
      struct file *file = file_open (struct inode *);
      break;
    }

    case SYS_FILESIZE: {
      verify_args(1);
      f->eax = filesize((int) args[1]);
      break;
    }

    case SYS_READ: {
      verify_args(3);
      f->eax = read((int) fd, (void *) buffer, (unsigned) length);
      break;
    }

    case SYS_SEEK: {
      verify_args(2);
      seek((int) args[1], (unsigned) args[2]);
      break;
    }

    case SYS_TELL: {
      verify_args(1);
      f->eax = file_tell (struct file *);
      break;
    }

    case SYS_CLOSE: {
      verify_args(1);
      close((int) args[1]);
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
  if (fd == STDOUT_FILENO) {
    putbuf(buffer, size);
    return size;
  } else if (fd == STDIN_FILENO)
    return -1;
  else {
    struct file *file;
    lock_acquire(&file_lock);
    int read_size = (int) file_write(file, buffer, (off_t) size);
    lock_release(&file_lock);
    return read_size;
  }
}

/* Syscall handler for when a syscall create is invoked. */
static bool create (const char *file, unsigned initial_size) { // DONE
  return filesys_create(file, (off_t) initial_size); 
}

/* Syscall handler for when a syscall remove is invoked. */
static bool remove (const char *file) {
  return filesys_remove(file);
}

/* Syscall handler for when a syscall open is invoked. */
static int open (const char *file_) { // DONE
  struct file *file = filesys_open(file);
  if (file == NULL)
    return -1;
  struct file_wrapper *f = (struct file_wrapper *) malloc(sizeof(struct file_wrapper));
  f->file = file;
  list_push_back(&thread_current()->file_wrappers, &f->list_elem);
  lock_acquire(&fd_lock);
  int fd = curr_fd++;
  lock_release(&fd_lock);
  f->fd = fd;
  hash_insert(&hash_table, &f->hash_elem);
  return fd;
}

/* Syscall handler for when a syscall filesize is invoked. */
static int filesize (int fd) { // DONE
  return (int) file_length (fd_to_file(fd));
}

/* Syscall handler for when a syscall read is invoked. */
static int read (int fd, void *buffer, unsigned length) {
  if (fd == STDIN_FILENO) {
    int i;
    for (i = 0; i < length; i++)
      input_getc();
    return length;
  } else if (fd == STDOUT_FILENO)
    return -1;
  else {
    lock_acquire(&file_lock);
    int size = (int) file_read(fd_to_file(fd), buffer, (off_t) length);
    lock_release(&file_lock);
    return size;
  }
}

/* Syscall handler for when a syscall seek is invoked. */
static void seek (int fd, unsigned position) { // DONE
  file_seek (fd_to_file(fd), (off_t) position);
}

/* Syscall handler for when a syscall tell is invoked. */
static unsigned tell (int fd) { // DONE
  return (unsigned) file_tell(fd_to_file(fd));
}

/* Syscall handler for when a syscall close is invoked. */
static void close (int fd) {
  file_close(fd_to_file(fd));
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

struct file *
fd_to_file (int fd) {
  struct list list = thread_current()->fd_list;
  struct list_elem e;
  for (e = list_begin (&list); e != list_end (&list); e = list_next (e)) {
      struct file_wrapper *file = list_entry (e, struct file_wrapper, elem);
      if (file -> fd == fd) {
        return file -> file;
      }
  }
  return NULL;
}
