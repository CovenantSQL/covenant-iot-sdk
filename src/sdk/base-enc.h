#ifndef COVENANTSQL_BASE_ENC_H
#define COVENANTSQL_BASE_ENC_H

#include "buffer.h"

#include <inttypes.h>

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

void encode_uint8(struct Buffer *const dest, uint8_t value);
int decode_uint8(struct Buffer *const src, uint8_t *const value);
int decode_uint8_pointer(struct Buffer *const src, uint8_t **value);
void encode_uint16(struct Buffer *const dest, uint16_t value);
int decode_uint16(struct Buffer *const src, uint16_t *const value);
int decode_uint16_pointer(struct Buffer *const src, uint16_t **value);
void encode_uint32(struct Buffer *const dest, uint32_t value);
int decode_uint32(struct Buffer *const src, uint32_t *const value);
int decode_uint32_pointer(struct Buffer *const src, uint32_t **value);
void encode_uint64(struct Buffer *const dest, uint64_t value);
int decode_uint64(struct Buffer *const src, uint64_t *const value);
int decode_uint64_pointer(struct Buffer *const src, uint64_t **value);
void encode_string(struct Buffer *const dest, const char *src);
int decode_string(struct Buffer *const src, char **dest);

struct Blob {
    size_t count;
    uint8_t buffer[];
};

void encode_blob(struct Buffer *const dest, const struct Blob *src);
int decode_blob(struct Buffer *const src, struct Blob **dest);

void encode_float(struct Buffer *const dest, float value);
int decode_float(struct Buffer *const src, float *value);
int decode_float_pointer(struct Buffer *const src, float **value);
void encode_double(struct Buffer *const dest, double value);
int decode_double(struct Buffer *const src, double *value);
int decode_double_pointer(struct Buffer *const src, double **value);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* COVENANTSQL_BASE_ENC_H */
