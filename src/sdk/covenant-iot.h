#ifndef COVENANTSQL_IOT_H
#define COVENANTSQL_IOT_H

#include <MQTTClient.h>
#include <json-c/json.h>
#include <sqlite3.h>

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

struct LogEntry;

void log_entry_init(struct LogEntry *const log_entry);
void log_entry_free(struct LogEntry *const log_entry);
int encode_log_entry_json(struct json_object *const dest, const struct LogEntry *src);
int decode_log_entry_json(const struct json_object *obj, struct LogEntry **dest);

struct LocalLog;

void local_log_init(struct LocalLog *const local_log);
void local_log_free(struct LocalLog *const local_log);
int local_log_append(struct LocalLog *const local_log, struct LogEntry *log_entry);

int cql_open(
    const char *filename,
    const char *client_id,
    const char *address,
    const char *user,
    const char *password,
    const char *topic,
    struct LocalLog **dest);

int cql_exec(struct LocalLog *const local_log, const char *sql,
             int (*callback) (void *, int, char **, char **), void *arg, char **errmsg);

int cql_sync_upstream(struct LocalLog *const local_log);

int cql_publish(struct LocalLog *const local_log);

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif /* COVENANTSQL_IOT_H */
