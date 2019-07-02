#ifndef COVENANTSQL_EXAMPLE_CONFIG_H
#define COVENANTSQL_EXAMPLE_CONFIG_H

#define ADDRESS         "tcp://192.168.2.100:18888"
#define USER            "client_test"
#define PASSWORD        "laodouya"
#define CLIENTID        "client_test"
#define TOPIC_NEWEST    "/cql/miner/000003f49592f83d0473bddb70d543f1096b4ffed5e5f942a3117e256b7052b8/0a10b74439f2376d828c9a70fd538dac4b69e0f4065424feebc0f5dbc8b34872/newest"
#define TOPIC           "/cql/client/client_test/0a10b74439f2376d828c9a70fd538dac4b69e0f4065424feebc0f5dbc8b34872/write"
#define QOS         1
#define TIMEOUT     10000L
// A test payload
#define PAYLOAD         "{ \"client_id\": \"client_test\", \"client_seq\": 0, \"events\": [ { \"pattern\": \"CREATE TABLE IF NOT EXISTS t1 (c1 int primary key, c2 text)\", \"args\": []  }, { \"pattern\": \"INSERT INTO t1 (c1, c2) VALUES (?, ?), (?, ?)\", \"args\": [ { \"name\": \"\", \"value\": 1 }, { \"name\": \"\", \"value\": \"text1\" }, { \"name\": \"\", \"value\": 2 }, { \"name\": \"\", \"value\": \"text2\" }  ]  }  ]  }"

#endif /* COVENANTSQL_EXAMPLE_CONFIG_H */
