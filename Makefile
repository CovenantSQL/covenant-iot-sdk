BUILD_DIR := build

LDFLAGS := -lsqlcipher -ljson-c -lpaho-mqtt3cs
CFLAGS_INC :=
CFLAGS := -g -Wall -D_DEFAULT_SOURCE $(CFLAGS_INC)

src = $(wildcard src/sdk/*.c)
obj = $(src:.c=.o)

local-log-test: $(obj)
	$(CC) -o build/test/$@ $^ $(LDFLAGS) src/sdk/test/local-log-test.c

mqtt-test: $(obj)
	$(CC) -o build/test/$@ $^ $(LDFLAGS) src/sdk/test/mqtt-test.c

mqtt-writer-test: $(obj)
	$(CC) -o build/test/$@ $^ $(LDFLAGS) src/sdk/test/mqtt-writer-test.c

sqlite3-test: $(obj)
	$(CC) -o build/test/$@ $^ $(LDFLAGS) src/sdk/test/sqlite3-test.c

.PHONY: clean
clean:
	rm -f $(obj) local-log-test
