#include "config.h"
#include "../covenant-iot.h"

int main()
{
    struct json_object *obj = json_object_from_file("./entries.json");
    struct LogEntry *log_entry;
    int rc = decode_log_entry_json(obj, &log_entry);
    json_object_put(obj); // release object
    if (rc != 0) {
        return rc;
    }

    struct LocalLog *ll;
    rc = cql_open("./local-test", CLIENTID, ADDRESS, USER, PASSWORD, TOPIC, &ll);
    if (rc != 0) {
        log_entry_free(log_entry);
        return rc;
    }
    rc = local_log_append(ll, log_entry);
    if (rc != 0) {
        log_entry_free(log_entry);
        return rc;
    }
    log_entry_free(log_entry);
    local_log_free(ll);
    ll = NULL;

    // Test reopen
    rc = cql_open("./local-test", CLIENTID, ADDRESS, USER, PASSWORD, TOPIC, &ll);
    if (rc != 0) {
        return rc;
    }
    local_log_free(ll);
    ll = NULL;
    return 0;
}

