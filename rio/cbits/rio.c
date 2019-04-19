
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

int __o_tmpfile( void )
{
  return __O_TMPFILE;
}

int __at_fdcwd( void )
{
  return AT_FDCWD;
}

int __at_symlink_follow( void )
{
  return AT_SYMLINK_FOLLOW;
}


int __s_irusr( void )
{
  return S_IRUSR;
}

int __s_iwusr( void )
{
  return S_IWUSR;
}

