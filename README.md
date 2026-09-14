[![progress-banner](https://backend.codecrafters.io/progress/kafka/ba46f8d5-69bb-4a7f-ae8f-c9b2a64d30cf)](https://app.codecrafters.io/users/kahvecikaan?r=2qF)

# kafka-broker-go

A small Kafka broker written in Go. It speaks the Kafka wire protocol over TCP.
It reads and writes messages in Kafka's on-disk log format. This project is a
solution to the CodeCrafters
["Build Your Own Kafka"](https://codecrafters.io/challenges/kafka) challenge.

The broker does not aim to be complete. It implements the parts needed to accept
records, store them on disk, and serve them back.

## What it supports

The broker answers four request types:

| API | Key | Max version | What it does |
| --- | --- | --- | --- |
| ApiVersions | 18 | 4 | Tells a client which APIs and versions the broker supports. |
| Fetch | 1 | 16 | Reads a partition's records from disk and returns them. |
| Produce | 0 | 11 | Validates the topic and partition, then writes the records to disk. |
| DescribeTopicPartitions | 75 | 0 | Returns topic and partition metadata. |

The broker listens on `0.0.0.0:9092`. It reads cluster metadata and partition
logs from `/tmp/kraft-combined-logs`.

## How to run

You need Go 1.26 or later.

```sh
./your_program.sh
```

The script builds the code and starts the broker. Use `codecrafters submit` to
run the challenge tests.

## Project layout

Each package has one job. Outer packages depend on inner ones, never the reverse.

```
app/main.go            Wires everything together and starts the server.
internal/server        Accepts TCP connections and frames messages.
internal/kafka         Parses the request header, routes by API key, and
                       runs the per-API handlers.
internal/protocol      Reads and writes Kafka wire primitives (Decoder/Encoder).
internal/metadata      Loads the cluster metadata log into an in-memory store.
internal/storage       Reads and writes partition log files on disk.
```

## How a request flows

1. `server` reads the 4-byte message size, then reads that many bytes.
2. `kafka` decodes the request header and picks the response header version.
3. `kafka` routes on the API key and runs the matching handler.
4. The handler builds a typed response and encodes it to bytes.
5. `server` prepends the 4-byte length and writes the response.

If the request is truncated, the broker drops the connection. It never sends a
partial response.

## On-disk layout

The broker uses the same directory layout as Kafka in KRaft mode.

```
/tmp/kraft-combined-logs/
├── __cluster_metadata-0/
│   └── 00000000000000000000.log      Cluster metadata. Read at startup.
└── <topic>-<partition>/
    └── 00000000000000000000.log      Partition records. Read by Fetch,
                                      written by Produce.
```

## Design notes

**Records are opaque bytes.** A RecordBatch has the same format on disk and on
the wire. So Fetch reads the log file and sends the bytes without change. Produce
takes the bytes from the request and writes them without change. The broker does
not parse or rebuild a RecordBatch to move messages. This mirrors Kafka's
zero-copy design.

**Metadata lives in memory.** The broker reads the metadata log once at startup.
It builds a store of topics, indexed by name and by UUID, with each topic's
partitions attached. Handlers query this store. They do not read the metadata
file again. This matches how a real broker keeps a metadata cache, and it is the
one place that does parse a RecordBatch, because it needs the record contents.

**The decoder uses a sticky error.** Once a read fails, later reads do nothing
and the error stays set. So a handler reads fields as a plain sequence. It checks
the error once at the end.

**Response header versions differ per API.** Flexible APIs (Produce, Fetch,
DescribeTopicPartitions) use response header v1, which adds a tag buffer.
ApiVersions uses response header v0. The router sets the version per API.

## Limitations

The broker is a learning project. It leaves out parts a real broker needs:

- It does not verify the CRC of a RecordBatch.
- It assigns `base_offset` 0 and reports a high watermark of 0. It does not track
  real offsets yet.
- It loads metadata once. It does not follow live metadata changes.
- It reads only the first log segment per partition.
- It does not replicate, and it runs as a single node.
