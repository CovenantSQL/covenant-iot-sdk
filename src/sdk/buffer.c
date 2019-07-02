#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <endian.h>
#include <unistd.h>

#include "buffer.h"

#define PAGE_SIZE 4096

void buffer_ensure(struct Buffer *const dest, size_t count)
{
    if (count <= dest->size - dest->offset) {
        return;
    }
    if (dest->buffer == NULL || dest->size == 0) {
        dest->read_p = 0;
        dest->offset = 0;
        dest->size = PAGE_SIZE;
    }
    while (count > dest->size - dest->offset) {
        dest->size *= 2;
    }
    dest->buffer = realloc(dest->buffer, dest->size);
}

void buffer_write(struct Buffer *const dest, const void *src, size_t count)
{
    buffer_ensure(dest, count);
    memcpy(dest->buffer+dest->offset, src, count);
    dest->offset += count;
}

int buffer_flush(struct Buffer *const src)
{
    if (src->fp == NULL || src->read_p > src->offset) {
        return -1;
    }
    size_t rc = fwrite(src->buffer+src->read_p, src->offset-src->read_p, 1, src->fp);
    if (rc < 1 || ferror(src->fp) != 0) {
        // IO error
        return -1;
    }
    src->read_p = src->offset;
    return 0;
}

int buffer_read(struct Buffer *const src, void *const dest, size_t count)
{
    if (src->read_p > src->offset || src->offset - src->read_p < count) {
        if (src->fp == NULL) {
            return -1;
        }
        // Read page
        buffer_ensure(src, PAGE_SIZE);
        size_t rc = fread(src->buffer+src->offset, 1, PAGE_SIZE, src->fp);
        if (rc < PAGE_SIZE && ferror(src->fp) != 0) {
            // IO error
            printf("failed to read data: io error\n");
            return -1;
        }
        src->offset += rc;
        if (src->read_p > src->offset || src->offset - src->read_p < count) {
            // No enough data
            printf("file has no enough data\n");
            return -1;
        }
    }
    memcpy(dest, src->buffer+src->read_p, count);
    src->read_p += count;
    return 0;
}

void buffer_init(struct Buffer *const buffer)
{
    buffer->fp = NULL;
    buffer->buffer = NULL;
    buffer->read_p = 0;
    buffer->offset = 0;
    buffer->size = 0;
}

void buffer_free(struct Buffer *const buffer)
{
    free(buffer->buffer);
    free((void *)buffer);
}
