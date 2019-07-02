#include "base-enc.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <endian.h>
#include <unistd.h>

void encode_uint8(struct Buffer *const dest, uint8_t value)
{
    buffer_write(dest, (void *)(&value), sizeof(value));
}

int decode_uint8(struct Buffer *const src, uint8_t *const value)
{
    return buffer_read(src, (void *)value, sizeof(*value));
}

int decode_uint8_pointer(struct Buffer *const src, uint8_t **value)
{
    int rc;
    int8_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    uint8_t *dvalue = malloc(sizeof(*dvalue));
    *dvalue = bevalue;
    *value = dvalue;
    return 0;
}

void encode_uint16(struct Buffer *const dest, uint16_t value)
{
    int16_t bevalue = htobe16(value);
    buffer_write(dest, (void *)(&bevalue), sizeof(bevalue));
}

int decode_uint16(struct Buffer *const src, uint16_t *const value)
{
    int rc;
    int16_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    *value = be16toh(bevalue);
    return 0;
}

int decode_uint16_pointer(struct Buffer *const src, uint16_t **value)
{
    int rc;
    int16_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    uint16_t *dvalue = malloc(sizeof(*dvalue));
    *dvalue = be16toh(bevalue);
    *value = dvalue;
    return 0;
}

void encode_uint32(struct Buffer *const dest, uint32_t value)
{
    int32_t bevalue = htobe32(value);
    buffer_write(dest, (void *)(&bevalue), sizeof(bevalue));
}

int decode_uint32(struct Buffer *const src, uint32_t *const value)
{
    int rc;
    int32_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    *value = be32toh(bevalue);
    return 0;
}

int decode_uint32_pointer(struct Buffer *const src, uint32_t **value)
{
    int rc;
    int32_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    uint32_t *dvalue = malloc(sizeof(*dvalue));
    *dvalue = be32toh(bevalue);
    *value = dvalue;
    return 0;
}

void encode_uint64(struct Buffer *const dest, uint64_t value)
{
    uint64_t bevalue = htobe64(value);
    buffer_write(dest, (void *)(&bevalue), sizeof(bevalue));
}

int decode_uint64(struct Buffer *const src, uint64_t *const value)
{
    int rc;
    int64_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    *value = be64toh(bevalue);
    return 0;
}

int decode_uint64_pointer(struct Buffer *const src, uint64_t **value)
{
    int rc;
    int64_t bevalue;
    rc = buffer_read(src, (void *)(&bevalue), sizeof(bevalue));
    if (rc != 0) {
        return rc;
    }
    uint64_t *dvalue = malloc(sizeof(*dvalue));
    *dvalue = be64toh(bevalue);
    *value = dvalue;
    return 0;
}

void encode_string(struct Buffer *const dest, const char *src)
{
    size_t count = strlen(src);
    encode_uint32(dest, (uint32_t)count);
    buffer_write(dest, (void *)src, count);
}

int decode_string(struct Buffer *const src, char **dest)
{
    int rc;
    uint32_t count;
    rc = decode_uint32(src, &count);
    if (rc != 0) {
        return rc;
    }

    void *ddest = malloc((size_t)(count+1));
    memset(ddest, 0, (size_t)(count+1));
    rc = buffer_read(src, ddest, count);
    if (rc != 0) {
        free(ddest);
        return rc;
    }
    *dest = (char *)ddest;
    return 0;
}

void encode_blob(struct Buffer *const dest, const struct Blob *src)
{
    encode_uint32(dest, (uint32_t)src->count);
    buffer_write(dest, src->buffer, src->count);
}

int decode_blob(struct Buffer *const src, struct Blob **dest)
{
    int rc;
    uint32_t count;
    rc = decode_uint32(src, &count);
    if (rc != 0) {
        return rc;
    }

    struct Blob *ddest = malloc(sizeof(*ddest) + count);
    ddest->count = (size_t)count;
    rc = buffer_read(src, ddest->buffer, ddest->count);
    if (rc != 0) {
        free(ddest);
        return rc;
    }

    *dest = ddest;
    return 0;
}

void encode_float(struct Buffer *const dest, float value)
{
    encode_uint32(dest, *(uint32_t *)(&value));
}

int decode_float(struct Buffer *const src, float *value)
{
    return decode_uint32(src, (uint32_t *)value);
}

int decode_float_pointer(struct Buffer *const src, float **value)
{
    return decode_uint32_pointer(src, (uint32_t **)value);
}

void encode_double(struct Buffer *const dest, double value)
{
    encode_uint64(dest, *(uint64_t *)(&value));
}

int decode_double(struct Buffer *const src, double *value)
{
    return decode_uint64(src, (uint64_t *)value);
}

int decode_double_pointer(struct Buffer *const src, double **value)
{
    return decode_uint64_pointer(src, (uint64_t **)value);
}
