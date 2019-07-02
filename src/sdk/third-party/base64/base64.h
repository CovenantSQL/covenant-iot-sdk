#ifndef BASE64_H
#define BASE64_H

#include <stddef.h>

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned char u_char;

int
b64_ntop(u_char const *src, size_t srclength, char *target, size_t targsize);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* BASE64_H */
