#ifndef COVENANTSQL_BUFFER_H
#define COVENANTSQL_BUFFER_H

#include <MQTTClient.h>
#include <json-c/json.h>
#include <sqlite3.h>

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

struct Buffer {
    FILE *fp;
    void *buffer;
    size_t read_p;
    size_t offset;
    size_t size;
};

void buffer_ensure(struct Buffer *const dest, size_t count);
void buffer_write(struct Buffer *const dest, const void *src, size_t count);
int buffer_flush(struct Buffer *const src);
int buffer_read(struct Buffer *const src, void *const dest, size_t count);
void buffer_init(struct Buffer *const buffer);
void buffer_free(struct Buffer *const buffer);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* COVENANTSQL_BUFFER_H */
