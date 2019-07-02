#include <sqlite3.h>
#include <stdio.h>

int callback(void *NotUsed, int argc, char **argv, char **azColName);

int main(void)
{
    sqlite3* db;
    char *err_msg = 0;

    int rc = sqlite3_enable_shared_cache(1);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "cannot enable shared cache: %s\n", sqlite3_errstr(rc));
        return 1;
    }

    rc = sqlite3_open("test.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    char *init = "PRAGMA journal_mode=WAL;"
                 "PRAGMA read_uncommitted=1;";
    rc = sqlite3_exec(db, init, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "sql error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    char *sql = "DROP TABLE IF EXISTS Friends;"
                "BEGIN TRANSACTION;"
                "CREATE TABLE Friends(id INTEGER PRIMARY KEY, name text);"
                "INSERT INTO FRIENDS (name) VALUES ('tom'), ('rebecca');"
                "INSERT INTO FRIENDS (name) VALUES ('jim');"
                "INSERT INTO FRIENDS (name) VALUES ('robert');";
    rc = sqlite3_exec(db, sql, 0, 0, &err_msg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "sql error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    char *qs = "SELECT * FROM Friends;";

    rc = sqlite3_exec(db, qs, callback, 0, &err_msg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "sql error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(db);
        return 1;
    }

    sqlite3_close(db);
    return 0;
}

int callback(void *NotUsed, int argc, char **argv, char **azColName)
{
    NotUsed = 0;

    for (int i = 0; i < argc; i ++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }

    return 0;
}
