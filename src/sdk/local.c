#include "buffer.h"
#include "base-enc.h"
#include "local.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <endian.h>
#include <unistd.h>

enum Types {
    Null, String, Int, Float, Blob,
};

struct Argument {
    char* name;
    void* value;
    enum Types type;
};

void argument_init(struct Argument *const arg)
{
    arg->name = NULL;
    arg->value = NULL;
}

void argument_free(struct Argument *const arg)
{
    free((void *)arg->name);
    free((void *)arg->value);
    free((void *)arg);
}

void encode_argument(struct Buffer *const dest, struct Argument *arg)
{
    encode_string(dest, arg->name);
    encode_uint8(dest, (uint8_t)arg->type);

    switch (arg->type) {
    case Null:
        break;
    case String:
        encode_string(dest, (char *)arg->value);
        break;
    case Int:
        encode_uint64(dest, *(uint64_t *)arg->value);
        break;
    case Float:
        encode_double(dest, *(double *)arg->value);
        break;
    case Blob:
        encode_blob(dest, (struct Blob*)arg->value);
        break;
    default:
        break;
    }
}

int encode_argument_json(struct json_object* dest, const struct Argument *src)
{
    int rc;

    if ((rc = json_object_object_add(dest, "name", json_object_new_string(src->name))) != 0 ||
        (rc = json_object_object_add(dest, "type", json_object_new_int((int32_t)src->type)) != 0)) {
        return rc;
    }

    struct Blob *blob = NULL;
    switch (src->type) {
    case Null:
        rc = json_object_object_add(dest, "value", NULL);
        break;
    case String:
        rc = json_object_object_add(dest, "value", json_object_new_string((char *)src->value));
        break;
    case Int:
        rc = json_object_object_add(dest, "value", json_object_new_int64(*(int64_t *)src->value));
        break;
    case Float:
        rc = json_object_object_add(dest, "value", json_object_new_double(*(double *)src->value));
        break;
    case Blob:
        blob = (struct Blob *)src->value;
        rc = json_object_object_add(dest, "value", json_object_new_string_len((char *)blob->buffer, blob->count));
        break;
    default:
        break;
    }

    return rc;
}

int decode_argument(struct Buffer *const src, struct Argument **dest)
{
    int rc;
    struct Argument *ddest = malloc(sizeof(*ddest));
    argument_init(ddest);

    rc = decode_string(src, &ddest->name);
    if (rc != 0) {
        argument_free(ddest);
        return rc;
    }

    uint8_t type;
    rc = decode_uint8(src, &type);
    if (rc != 0) {
        argument_free(ddest);
        return rc;
    }
    ddest->type = type;

    switch (ddest->type) {
    case Null:
        break;
    case String:
        rc = decode_string(src, (char **)(&ddest->value));
        break;
    case Int:
        rc = decode_uint64_pointer(src, (uint64_t **)(&ddest->value));
        break;
    case Float:
        rc = decode_double_pointer(src, (double **)(&ddest->value));
        break;
    case Blob:
        rc = decode_blob(src, (struct Blob **)(&ddest->value));
        break;
    default:
        break;
    }
    if (rc != 0) {
        argument_free(ddest);
        return rc;
    }

    *dest = ddest;
    return 0;
}

int decode_argument_json(const struct json_object *obj, struct Argument **dest)
{
    json_bool ok;
    struct json_object *value = NULL;
    struct Argument *ddest = malloc(sizeof(*ddest));
    argument_init(ddest);

    // "name", nullable
    ok = json_object_object_get_ex(obj, "name", &value);
    if (ok == 0 || json_object_is_type(value, json_type_null) != 0) {
        printf("field name is null\n");
    } else {
        if (json_object_is_type(value, json_type_string) == 0) {
            printf("unexpected name type\n");
            argument_free(ddest);
            return 1;
        }
        ddest->name = strndup(json_object_get_string(value),
                              (size_t)json_object_get_string_len(value));
    }

    // "type"
    if ((ok = json_object_object_get_ex(obj, "type", &value)) == 0) {
        printf("field type not found\n");
        argument_free(ddest);
        return 1;
    }
    if (json_object_is_type(value, json_type_int) == 0) {
        printf("unexpected type type\n");
        argument_free(ddest);
        return 1;
    }
    ddest->type = (enum Types)json_object_get_int(value);

    // set value by type
    if (ddest->type == Null) {
        *dest = ddest;
        return 0;
    }

    if ((ok = json_object_object_get_ex(obj, "value", &value)) == 0) {
        printf("field value not found\n");
        argument_free(ddest);
        return 1;
    }

    switch (ddest->type) {
    case String:
        if (json_object_is_type(value, json_type_string) == 0) {
            printf("unexpected object type\n");
            argument_free(ddest);
            return 1;
        }
        ddest->value = strndup(json_object_get_string(value),
                               (size_t)json_object_get_string_len(value));
        break;
    case Int:
        if (json_object_is_type(value, json_type_int) == 0) {
            printf("unexpected object type\n");
            argument_free(ddest);
            return 1;
        }
        int64_t iv = json_object_get_int64(value);
        ddest->value = malloc(sizeof(iv));
        memcpy(ddest->value, &iv, sizeof(iv));
        break;
    case Float:
        if (json_object_is_type(value, json_type_double) == 0) {
            printf("unexpected object type\n");
            argument_free(ddest);
            return 1;
        }
        double fv = json_object_get_double(value);
        ddest->value = malloc(sizeof(fv));
        memcpy(ddest->value, &fv, sizeof(fv));
        break;
    case Blob:
        // TODO(leventeliu): base64 decoding.
        if (json_object_is_type(value, json_type_string) == 0) {
            printf("unexpected object type\n");
            argument_free(ddest);
            return 1;
        }
        ddest->value = strndup(json_object_get_string(value),
                               (size_t)json_object_get_string_len(value));
        break;
    default:
        break;
    }

    *dest = ddest;
    return 0;
}

struct Event {
    char* pattern;
    size_t count;
    struct Argument* args[];
};

void event_init(struct Event *const event)
{
    event->pattern = NULL;
    event->count = 0;
}

void event_free(struct Event *const event)
{
    free(event->pattern);
    for (size_t i = 0; i < event->count; i++) {
        free(event->args[i]);
    }
    free(event);
}

void encode_event(struct Buffer *const dest, const struct Event *src)
{
    encode_string(dest, src->pattern);
    encode_uint32(dest, (uint32_t)src->count);
    for (size_t i = 0; i < src->count; i++) {
        encode_argument(dest, src->args[i]);
    }
}

int encode_event_json(struct json_object* dest, const struct Event *src)
{
    int rc;
    struct json_object *args;
    struct json_object *arg;

    if ((rc = json_object_object_add(dest, "pattern", json_object_new_string(src->pattern))) != 0 ||
        (rc = json_object_object_add(dest, "args", (args = json_object_new_array())) != 0)) {
        return rc;
    }

    for (size_t i = 0; i < src->count; i++) {
        if ((rc = json_object_array_add(args, (arg = json_object_new_object()))) != 0) {
            return rc;
        }
        if ((rc = encode_argument_json(arg, src->args[i])) != 0) {
            return rc;
        }
    }

    return 0;
}

int decode_event(struct Buffer *const src, struct Event **dest)
{
    int rc;
    struct Event *ddest = malloc(sizeof(*ddest));
    event_init(ddest);

    rc = decode_string(src, &ddest->pattern);
    if (rc != 0) {
        event_free(ddest);
        return rc;
    }

    uint32_t count;
    rc = decode_uint32(src, &count);
    if (rc != 0) {
        event_free(ddest);
        return rc;
    }

    ddest->count = (size_t)count;
    ddest = realloc((void *)ddest, sizeof(*ddest) + count*sizeof(void *));
    for (size_t i = 0; i < ddest->count; i++) {
        rc = decode_argument(src, &(ddest->args[i]));
        if (rc != 0) {
            event_free(ddest);
            return rc;
        }
    }

    *dest = ddest;
    return 0;
}

int decode_event_json(const struct json_object *obj, struct Event **dest)
{
    int rc;
    json_bool ok;
    struct json_object *value = NULL;
    struct Event *ddest = malloc(sizeof(*ddest));
    event_init(ddest);

    // "pattern"
    if ((ok = json_object_object_get_ex(obj, "pattern", &value)) == 0) {
        printf("field pattern not found\n");
        event_free(ddest);
        return 1;
    }
    if (json_object_is_type(value, json_type_string) == 0) {
        printf("unexpected pattern type\n");
        event_free(ddest);
        return 1;
    }
    ddest->pattern = strndup(json_object_get_string(value),
                             (size_t)json_object_get_string_len(value));
    printf("decode pattern: %s\n", ddest->pattern);

    // "args", nullable
    ok = json_object_object_get_ex(obj, "args", &value);
    if (ok == 0 || json_object_is_type(value, json_type_null) != 0) {
        printf("field args is null\n");
    } else {
        if (json_object_is_type(value, json_type_array) == 0) {
            printf("unexpected args type\n");
            event_free(ddest);
            return 1;
        }

        size_t args_count = json_object_array_length(value);
        struct json_object *iter = NULL;
        ddest->count = args_count;
        ddest = realloc((void *)ddest, sizeof(*ddest) + args_count*sizeof(void *));
        for (size_t i = 0; i < ddest->count; i++) {
            iter = json_object_array_get_idx(value, i);
            if ((rc = decode_argument_json(iter, &(ddest->args[i]))) != 0) {
                printf("failed to decode arg at index %zd\n", i);
                event_free(ddest);
                return rc;
            }
        }
    }

    *dest = ddest;
    return 0;
}

struct LogEntry {
    uint64_t block_id;
    uint64_t block_index;
    char *client_id;
    uint64_t seq;
    size_t count;
    struct Event* events[];
};

void log_entry_init(struct LogEntry *const log_entry)
{
    log_entry->client_id = NULL;
    log_entry->count = 0;
}

void log_entry_free(struct LogEntry *const log_entry)
{
    free(log_entry->client_id);
    for (size_t i = 0; i < log_entry->count; i++) {
        event_free(log_entry->events[i]);
    }
    free(log_entry);
}

void encode_log_entry(struct Buffer *const dest, const struct LogEntry *src)
{
    encode_uint64(dest, src->block_id);
    encode_uint64(dest, src->block_index);
    encode_string(dest, src->client_id);
    encode_uint64(dest, src->seq);
    encode_uint32(dest, (uint32_t)src->count);
    for (size_t i = 0; i < src->count; i++) {
        encode_event(dest, src->events[i]);
    }
}

int encode_log_entry_json(struct json_object *const dest, const struct LogEntry *src)
{
    int rc;
    struct json_object *events;
    struct json_object *event;

    if ((rc = json_object_object_add(dest, "client_id", json_object_new_string(src->client_id))) != 0 ||
        (rc = json_object_object_add(dest, "client_seq", json_object_new_int64((int64_t)src->seq)) != 0) ||
        (rc = json_object_object_add(dest, "events", (events = json_object_new_array())) != 0)) {
        return rc;
    }

    for (size_t i = 0; i < src->count; i++) {
        if ((rc = json_object_array_add(events, (event = json_object_new_object()))) != 0) {
            return rc;
        }
        if ((rc = encode_event_json(event, src->events[i])) != 0) {
            return rc;
        }
    }

    return 0;
}

int decode_log_entry(struct Buffer *const src, struct LogEntry **dest)
{
    int rc;
    struct LogEntry *ddest = malloc(sizeof(*ddest));
    log_entry_init(ddest);

    if ((rc = decode_uint64(src, &ddest->block_id)) != 0
        || (rc = decode_uint64(src, &ddest->block_index) != 0)
        || (rc = decode_string(src, &ddest->client_id) != 0)
        || (rc = decode_uint64(src, &ddest->seq) != 0)) {
        log_entry_free(ddest);
        return rc;
    }

    uint32_t count;
    rc = decode_uint32(src, &count);
    if (rc != 0) {
        log_entry_free(ddest);
        return rc;
    }

    ddest->count = (size_t)count;
    ddest = realloc((void *)ddest, sizeof(*ddest) + count*sizeof(void *));
    for (size_t i = 0; i < ddest->count; i++) {
        rc = decode_event(src, &(ddest->events[i]));
        if (rc != 0) {
            log_entry_free(ddest);
            return rc;
        }
    }

    *dest = ddest;
    return 0;
}

int parse_json_from_payload(
    const void *payload, int payloadlen, struct json_object **dest)
{
    // Parse JSON payload
    struct json_tokener *tok = json_tokener_new();
    struct json_object *obj = json_tokener_parse_ex(tok, (const char *)payload, payloadlen);
    enum json_tokener_error jerr = json_tokener_get_error(tok);
    json_tokener_free(tok);

    if (obj == NULL) {
        printf("failed to parse json object: %s\n", json_tokener_error_desc(jerr));
        return 1;
    }

    *dest = obj;
    return 0;
}

int decode_log_entry_json(const struct json_object *obj, struct LogEntry **dest)
{
    // Get fields
    int rc;
    json_bool ok;
    struct json_object *value = NULL;
    struct LogEntry *ddest = malloc(sizeof(*ddest));
    log_entry_init(ddest);

    // "client_id", nullable
    ok = json_object_object_get_ex(obj, "client_id", &value);
    if (ok == 0 || json_object_is_type(value, json_type_null) != 0) {
        printf("field client_id is null\n");
    } else {
        if (json_object_is_type(value, json_type_string) == 0) {
            printf("unexpected client_id type\n");
            log_entry_free(ddest);
            return 1;
        }
        ddest->client_id = strndup(json_object_get_string(value),
                                   (size_t)json_object_get_string_len(value));
        printf("parsed client_id %s\n", ddest->client_id);
    }

    // "client_seq", nullable
    ok = json_object_object_get_ex(obj, "client_seq", &value);
    if (ok == 0 || json_object_is_type(value, json_type_null) != 0) {
        printf("field client_seq is null\n");
    } else {
        if (json_object_is_type(value, json_type_int) == 0) {
            printf("unexpected client_seq type\n");
            log_entry_free(ddest);
            return 1;
        }
        ddest->seq = (uint64_t)json_object_get_int64(value);
        printf("parsed client_seq %" PRIx64 "\n", ddest->seq);
    }

    // "block_id"
    if ((ok = json_object_object_get_ex(obj, "block_id", &value)) == 0) {
        printf("field block_id not found\n");
        log_entry_free(ddest);
        return 1;
    }
    if (json_object_is_type(value, json_type_int) == 0) {
        printf("unexpected block_id type\n");
        log_entry_free(ddest);
        return 1;
    }
    ddest->block_id = (uint64_t)json_object_get_int64(value);

    // "block_index"
    if ((ok = json_object_object_get_ex(obj, "block_index", &value)) == 0) {
        printf("field block_index not found\n");
        log_entry_free(ddest);
        return 1;
    }
    if (json_object_is_type(value, json_type_int) == 0) {
        printf("unexpected block_index type\n");
        log_entry_free(ddest);
        return 1;
    }
    ddest->block_index = (uint64_t)json_object_get_int64(value);

    // "events", nullable
    ok = json_object_object_get_ex(obj, "events", &value);
    if (ok == 0 || json_object_is_type(value, json_type_null) != 0) {
        printf("field events is null\n");
    } else {
        if (json_object_is_type(value, json_type_array) == 0) {
            printf("unexpected events type\n");
            log_entry_free(ddest);
            return 1;
        }

        size_t event_count = json_object_array_length(value);
        struct json_object *iter = NULL;
        ddest->count = event_count;
        ddest = realloc((void *)ddest, sizeof(*ddest) + event_count*sizeof(void *));
        printf("parsed events: count = %zd\n", event_count);
        for (size_t i = 0; i < ddest->count; i++) {
            iter = json_object_array_get_idx(value, i);
            if ((rc = decode_event_json(iter, &(ddest->events[i]))) != 0) {
                printf("failed to decode event at index %zd\n", i);
                log_entry_free(ddest);
                return rc;
            }
        }
    }

    *dest = ddest;
    return 0;
}

#define LOCAL_LOG_MAGIC     0x2e43514c
#define LOCAL_LOG_VERSION   0x01

struct LocalLogHeader {
    uint32_t magic;
    uint32_t version;
    // local data version
    uint64_t block_id;
    uint64_t block_index;
    // next sequence to publish
    uint64_t next_publish;
    // next sequence for new events
    uint64_t sequence;
    uint32_t entries;
    uint16_t salt;
    uint16_t checksum;
};

void encode_local_log_header(struct Buffer *const dest, const struct LocalLogHeader *src)
{
    encode_uint32(dest, src->magic);
    encode_uint32(dest, src->version);
    encode_uint64(dest, src->block_id);
    encode_uint64(dest, src->block_index);
    encode_uint64(dest, src->next_publish);
    encode_uint64(dest, src->sequence);
    encode_uint32(dest, src->entries);
    encode_uint16(dest, src->salt);
    encode_uint16(dest, src->checksum);
}

int decode_local_log_header(struct Buffer *const src, struct LocalLogHeader **dest)
{
    int rc;
    struct LocalLogHeader *ddest = malloc(sizeof(*ddest));
    if ((rc = decode_uint32(src, &ddest->magic)) != 0
        || (rc = decode_uint32(src, &ddest->version)) != 0
        || (rc = decode_uint64(src, &ddest->block_id)) != 0
        || (rc = decode_uint64(src, &ddest->block_index)) != 0
        || (rc = decode_uint64(src, &ddest->next_publish)) != 0
        || (rc = decode_uint64(src, &ddest->sequence)) != 0
        || (rc = decode_uint32(src, &ddest->entries)) != 0
        || (rc = decode_uint16(src, &ddest->salt)) != 0
        || (rc = decode_uint16(src, &ddest->checksum)) != 0) {
        free(ddest);
        return rc;
    }

    *dest = ddest;
    return 0;
}

struct LocalLog {
    char *filename;
    char *client_id;
    char *address;
    char *user;
    char *password;
    char *topic;

    FILE *fp;
    sqlite3 *db;

    struct LocalLogHeader *header;
};

void local_log_init(struct LocalLog *const local_log)
{
    local_log->filename = NULL;
    local_log->client_id = NULL;
    local_log->address = NULL;
    local_log->user = NULL;
    local_log->password = NULL;
    local_log->topic = NULL;

    local_log->fp = NULL;
    local_log->db = NULL;

    local_log->header = NULL;
}

void local_log_free(struct LocalLog *const local_log)
{
    free(local_log->filename);
    free(local_log->client_id);
    free(local_log->address);
    free(local_log->user);
    free(local_log->password);
    free(local_log->topic);

    fclose(local_log->fp);
    sqlite3_close(local_log->db);

    free(local_log->header);

    free(local_log);
}

int open_and_init_db(const char *filename, sqlite3 **dest)
{
    sqlite3* db;
    char *err_msg = 0;

    int rc = sqlite3_enable_shared_cache(1);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "cannot enable shared cache: %s\n", sqlite3_errstr(rc));
        return 1;
    }

    rc = sqlite3_open(filename, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    char *init = "PRAGMA journal_mode=WAL;"
                 "PRAGMA read_uncommitted=1;"
                 "BEGIN TRANSACTION;";
    rc = sqlite3_exec(db, init, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sql error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    *dest = db;
    return 0;
}

int cql_open(
    const char *filename,
    const char *client_id,
    const char *address,
    const char *user,
    const char *password,
    const char *topic,
    struct LocalLog **dest)
{
    int rc;

    struct LocalLog *ddest = malloc(sizeof(*ddest));
    local_log_init(ddest);

    ddest->filename = malloc(strlen(filename) + strlen("-loc") + 1);
    strcpy(ddest->filename, filename);
    strcat(ddest->filename, "-loc");

    ddest->client_id = strdup(client_id);
    ddest->address = strdup(address);
    ddest->user = strdup(user);
    ddest->password = strdup(password);
    ddest->topic = strdup(topic);

    // open db file
    if ((rc = open_and_init_db(filename, &ddest->db)) != 0) {
        local_log_free(ddest);
        return rc;
    }

    struct Buffer *buffer = malloc(sizeof(*buffer));
    buffer_init(buffer);

    if (access(ddest->filename, F_OK) != 0) {
        // File does not exist, initialize header
        ddest->header = malloc(sizeof(*(ddest->header)));
        ddest->header->magic = LOCAL_LOG_MAGIC;
        ddest->header->version= LOCAL_LOG_VERSION;
        ddest->header->block_id= 0;
        ddest->header->block_index = 0;
        ddest->header->sequence = 0;
        ddest->header->entries = 0;
        // TODO(leventeliu): randomize salt and calculate checksum.
        ddest->header->salt= 0;
        ddest->header->checksum = 0;

        // Create file and write header
        ddest->fp = fopen(ddest->filename, "w+b");
        if (ddest->fp == NULL) {
            buffer_free(buffer);
            local_log_free(ddest);
            return -1;
        }

        buffer->fp = ddest->fp;
        encode_local_log_header(buffer, ddest->header);
        rc = buffer_flush(buffer);
        if (rc != 0) {
            buffer_free(buffer);
            local_log_free(ddest);
            return -1;
        }
    } else {
        ddest->fp = fopen(ddest->filename, "r+b");
        if (ddest->fp == NULL) {
            printf("failed to open file: io error\n");
            buffer_free(buffer);
            local_log_free(ddest);
            return -1;
        }
        buffer->fp = ddest->fp;

        // Read header
        rc = decode_local_log_header(buffer, &ddest->header);
        if (rc != 0) {
            printf("failed to decode header\n");
            buffer_free(buffer);
            local_log_free(ddest);
            return -1;
        }

        printf("local log header: magic = %" PRIx32 " version = %" PRIx32 " seq = %" PRId64
               " entries = %" PRId32"\n",
               ddest->header->magic,
               ddest->header->version,
               ddest->header->sequence,
               ddest->header->entries);

        // TODO(leventeliu): verify header.

        // Read and replay entries
        struct LogEntry *entry;
        char *errmsg = NULL;
        for (size_t i = 0; i < ddest->header->entries; i++) {
            rc = decode_log_entry(buffer, &entry);
            if (rc != 0) {
                printf("failed to decode entry at #%zd\n", i);
                buffer_free(buffer);
                local_log_free(ddest);
                return -1;
            }
            printf("read log entry from local log: block_id = %" PRIu64 " block_index = %" PRIu64
                   " seq = %" PRId64 " pattern = %s\n",
                   entry->block_id,
                   entry->block_index,
                   entry->seq,
                   entry->events[0]->pattern);
            rc = sqlite3_exec(ddest->db, entry->events[0]->pattern, NULL, NULL, &errmsg);
            if (rc != SQLITE_OK) {
                fprintf(stderr, "sql error: %s\n", errmsg);
                sqlite3_free(errmsg);
                buffer_free(buffer);
                local_log_free(ddest);
                return 1;
            }
            // TODO(leventeliu): verify entries.
            sqlite3_free(errmsg);
            log_entry_free(entry);
        }
    }

    buffer_free(buffer);
    *dest = ddest;
    return 0;
}

int local_log_append(struct LocalLog *const local_log, struct LogEntry *log_entry)
{
    int rc;
    struct Buffer *buffer = malloc(sizeof(*buffer));
    buffer_init(buffer);
    buffer->fp = local_log->fp;

    // Append log entry
    log_entry->seq = local_log->header->sequence; // overwrite sequence number
    encode_log_entry(buffer, log_entry);
    rc = buffer_flush(buffer);
    if (rc != 0) {
        buffer_free(buffer);
        return rc;
    }

    // Update header
    //
    // TODO(leventeliu): randomize salt and calculate checksum.
    local_log->header->entries++;
    local_log->header->sequence++;
    rc = fseek(local_log->fp, 0, SEEK_SET);
    encode_local_log_header(buffer, local_log->header);
    rc = buffer_flush(buffer);
    buffer_free(buffer);

    if (rc != 0) {
        return rc;
    }

    rc = fseek(local_log->fp, 0, SEEK_END);
    return rc;
}

int local_log_merge(
    struct LocalLog *const local_log,
    struct LogEntry *upstream[], size_t upstream_size,
    struct LogEntry ***merged, size_t *merged_size, size_t *commit_point)
{
    int rc;

    // Load local log entires
    rc = fseek(local_log->fp, 0, SEEK_SET);
    if (rc != 0) {
        return rc;
    }

    // Read header
    struct Buffer *buffer = malloc(sizeof(*buffer));
    buffer_init(buffer);

    rc = decode_local_log_header(buffer, &local_log->header);
    if (rc != 0) {
        printf("failed to decode header\n");
        buffer_free(buffer);
        return -1;
    }

    printf("local log header: magic = %" PRIx32 " version = %" PRIx32 " seq = %" PRId64
           " entries = %" PRId32"\n",
           local_log->header->magic,
           local_log->header->version,
           local_log->header->sequence,
           local_log->header->entries);

    // Read entries
    struct LogEntry **local_log_entries = malloc((size_t)local_log->header->entries * sizeof(void *));
    for (size_t i = 0; i < local_log->header->entries; i++) {
        local_log_entries[i] = NULL;
    }

    for (size_t i = 0; i < local_log->header->entries; i++) {
        rc = decode_log_entry(buffer, &local_log_entries[i]);
        if (rc != 0) {
            printf("failed to decode entry at #%zd\n", i);
            buffer_free(buffer);
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            return 1;
        }
        printf("read log entry from local log: block_id = %" PRIu64 " block_index = %" PRIu64 "\n",
               local_log_entries[i]->block_id,
               local_log_entries[i]->block_index);
    }
    buffer_free(buffer);

    // Start merge
    struct LogEntry **dmerged = malloc((size_t)(local_log->header->entries + upstream_size)*sizeof(void *));
    size_t local_pos = 0;
    size_t ups_pos = 0;
    size_t merged_pos = 0;

    while (ups_pos < upstream_size && (
               upstream[ups_pos]->block_id < local_log->header->block_id ||
               (upstream[ups_pos]->block_id == local_log->header->block_id &&
                upstream[ups_pos]->block_index <= local_log->header->block_index))) {
        printf("upstream log entry is older than local: upstream = %" PRId64 ":%" PRId64
               " local = %" PRId64 ":%" PRId64,
               upstream[ups_pos]->block_id,
               upstream[ups_pos]->block_index,
               local_log->header->block_id,
               local_log->header->block_index);
        log_entry_free(upstream[ups_pos]);
        ups_pos++;
    }

    while (ups_pos < upstream_size) {
        if (strcmp(local_log->client_id, upstream[ups_pos]->client_id) == 0) {
            while (local_pos < local_log->header->entries &&
                   local_log_entries[local_pos]->seq < upstream[ups_pos]->seq) {
                printf("upstream log entry appears to be greater than local,"
                       " maybe be skipped by miner: upstream = %s:%" PRId64
                       " local = %s:%" PRId64,
                       upstream[ups_pos]->client_id, upstream[ups_pos]->seq,
                       local_log->client_id,
                       local_log_entries[local_pos]->seq);
                log_entry_free(local_log_entries[local_pos++]); // free discarded object in local_log_entries
            }
            // match not found
            if (local_pos >= local_log->header->entries) {
                printf("upstream log entry matches local client_id,"
                       " but cannot be found in local log, abort merging"
                       " upstream = %s:%" PRId64 ", abort",
                       upstream[ups_pos]->client_id, upstream[ups_pos]->seq);
                // TODO(leventeliu): free objects.
                return 1;
            }
            if (local_log_entries[local_pos]->seq > upstream[ups_pos]->seq) {
                printf("upstream log entry matches local client_id,"
                       " but local sequence number is greater, abort merging"
                       " upstream = %s:%" PRId64 ", abort",
                       upstream[ups_pos]->client_id, upstream[ups_pos]->seq);
                // TODO(leventeliu): free objects.
                return 1;
            }
            // matched
            log_entry_free(local_log_entries[local_pos++]); // free duplicate object in local_log_entries
            dmerged[merged_pos++] = upstream[ups_pos];      // move from upstream to merged list
        } else {
            // unknown client source, move from upstream to merged list directly
            dmerged[merged_pos++] = upstream[ups_pos];
        }
        ups_pos++;
    }

    // move remaining objects in local_log_entries
    *commit_point = merged_pos; // committed logs by chain
    while (local_pos < local_log->header->entries) {
        dmerged[merged_pos++] = local_log_entries[local_pos];
        local_pos++;
    }
    *merged = dmerged;
    *merged_size = merged_pos;
    return 0;
}

int cql_exec(struct LocalLog *const local_log, const char *sql,
             int (*callback) (void *, int, char **, char **), void *arg, char **errmsg)
{
    int rc;
    struct Buffer *buffer = malloc(sizeof(*buffer));
    buffer_init(buffer);
    struct Event *event = malloc(sizeof(*event));
    event_init(event);
    event->pattern = strdup(sql);
    struct LogEntry *entry = malloc(sizeof(*entry) + 1*sizeof(void *));
    log_entry_init(entry);
    entry->client_id = strdup(local_log->client_id);
    entry->seq = local_log->header->sequence;
    entry->count = 1;
    entry->events[0] = event;

    rc = sqlite3_exec(local_log->db, sql, callback, arg, errmsg);
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = local_log_append(local_log, entry);
    if (rc != 0 && errmsg != NULL) {
        *errmsg = strdup("failed to append local log");
    }

    return rc;
}

int cql_publish(struct LocalLog *const local_log)
{
    int rc;

    // Load local log entires
    rc = fseek(local_log->fp, 0, SEEK_SET);
    if (rc != 0) {
        return rc;
    }

    // Read header
    struct Buffer *buffer = malloc(sizeof(*buffer));
    buffer_init(buffer);

    rc = decode_local_log_header(buffer, &local_log->header);
    if (rc != 0) {
        printf("failed to decode header\n");
        buffer_free(buffer);
        return -1;
    }

    printf("local log header: magic = %" PRIx32 " version = %" PRIx32 " seq = %" PRId64
           " entries = %" PRId32"\n",
           local_log->header->magic,
           local_log->header->version,
           local_log->header->sequence,
           local_log->header->entries);

    // Read entries
    struct LogEntry **local_log_entries = malloc((size_t)local_log->header->entries * sizeof(void *));
    for (size_t i = 0; i < local_log->header->entries; i++) {
        local_log_entries[i] = NULL;
    }

    for (size_t i = 0; i < local_log->header->entries; i++) {
        rc = decode_log_entry(buffer, &local_log_entries[i]);
        if (rc != 0) {
            printf("failed to decode entry at #%zd\n", i);
            buffer_free(buffer);
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            return 1;
        }
        printf("read log entry from local log: block_id = %" PRIu64 " block_index = %" PRIu64 "\n",
               local_log_entries[i]->block_id,
               local_log_entries[i]->block_index);
    }
    buffer_free(buffer);

    // initialize mqtt client for publish
    MQTTClient client;
    MQTTClient_connectOptions opts = MQTTClient_connectOptions_initializer;
    MQTTClient_message msg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    MQTTClient_create(&client, local_log->address, local_log->client_id,
                      MQTTCLIENT_PERSISTENCE_NONE, NULL);

    opts.keepAliveInterval = 20;
    opts.cleansession = 1;
    opts.username = local_log->user;
    opts.password = local_log->password;

    if ((rc = MQTTClient_connect(client, &opts)) != MQTTCLIENT_SUCCESS) {
        printf("Failed to connect, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    printf("Connect to broker succeeded\n");
    // encode json entries
    size_t index = 0;
    struct json_object *obj = NULL;

    for (; index < local_log->header->entries &&
         local_log_entries[index]->seq < local_log->header->next_publish; index++);
    for (; index < local_log->header->entries; index++) {
        obj = json_object_new_object();
        rc = encode_log_entry_json(obj, local_log_entries[index]);
        if (rc != 0) {
            printf("failed to encode entry at #%zd\n", index);
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            json_object_put(obj);
            return 1;
        }

        // publish message
        msg.payload = (void *)json_object_to_json_string_length(
                          obj, JSON_C_TO_STRING_SPACED, (size_t *)&msg.payloadlen);
        msg.qos = 1;
        msg.retained = 0;
        MQTTClient_publishMessage(client, local_log->topic, &msg, &token);
        rc = MQTTClient_waitForCompletion(client, token, 10000L);

        if (rc != 0) {
            printf("failed to publish message at #%zd\n", index);
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            json_object_put(obj);
            return 1;
        }

        json_object_put(obj);

        // Update header
        struct Buffer *headbuff = malloc(sizeof(*headbuff));
        buffer_init(headbuff);
        local_log->header->next_publish++;
        rc = fseek(local_log->fp, 0, SEEK_SET);
        if (rc != 0) {
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            return rc;
        }
        encode_local_log_header(headbuff, local_log->header);
        rc = buffer_flush(headbuff);
        buffer_free(headbuff);
        if (rc != 0) {
            printf("failed to update file header for #%zd\n", index);
            for (size_t i = 0; i < local_log->header->entries; i++) {
                log_entry_free(local_log_entries[i]);
            }
            free(local_log_entries);
            return rc;
        }
    }

    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);

    for (size_t i = 0; i < local_log->header->entries; i++) {
        log_entry_free(local_log_entries[i]);
    }
    free(local_log_entries);

    return 0;
}
