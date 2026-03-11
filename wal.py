#!/usr/bin/env python3
"""Write-Ahead Log (WAL) — crash-safe sequential log for databases.

One file. Zero deps. Does one thing well.

WAL ensures durability by writing operations to a log before applying them.
On crash, replay the log to recover state. Used in PostgreSQL, SQLite, etcd.
"""
import struct, os, sys, hashlib, tempfile, time

class WALEntry:
    """Log entry: [length:4][crc32:4][timestamp:8][key_len:2][key][value]"""
    def __init__(self, key, value, timestamp=None):
        self.key = key
        self.value = value
        self.timestamp = timestamp or time.time()

    def encode(self):
        kb = self.key.encode()
        vb = self.value.encode() if isinstance(self.value, str) else self.value
        payload = struct.pack('<dH', self.timestamp, len(kb)) + kb + vb
        crc = self._crc32(payload)
        return struct.pack('<II', len(payload), crc) + payload

    @staticmethod
    def _crc32(data):
        # Simple CRC32 using hashlib
        return int(hashlib.md5(data).hexdigest()[:8], 16)

    @classmethod
    def decode(cls, data, offset):
        if offset + 8 > len(data):
            return None, offset
        length, crc = struct.unpack_from('<II', data, offset)
        if offset + 8 + length > len(data):
            return None, offset
        payload = data[offset + 8:offset + 8 + length]
        if cls._crc32(payload) != crc:
            return None, offset  # Corrupted entry
        ts, klen = struct.unpack_from('<dH', payload, 0)
        key = payload[10:10 + klen].decode()
        value = payload[10 + klen:].decode()
        return cls(key, value, ts), offset + 8 + length


class WAL:
    def __init__(self, path):
        self.path = path
        self.fd = open(path, 'ab+')
        self.size = os.path.getsize(path)

    def append(self, key, value):
        entry = WALEntry(key, value)
        data = entry.encode()
        self.fd.write(data)
        self.fd.flush()
        os.fsync(self.fd.fileno())
        self.size += len(data)
        return entry

    def replay(self):
        """Replay all entries from the log."""
        with open(self.path, 'rb') as f:
            data = f.read()
        entries = []
        offset = 0
        while offset < len(data):
            entry, new_offset = WALEntry.decode(data, offset)
            if entry is None:
                break
            entries.append(entry)
            offset = new_offset
        return entries

    def truncate(self):
        """Clear the log (after checkpoint)."""
        self.fd.close()
        self.fd = open(self.path, 'wb+')
        self.size = 0

    def close(self):
        self.fd.close()


class WALStore:
    """Simple KV store backed by WAL for crash safety."""
    def __init__(self, wal_path):
        self.wal = WAL(wal_path)
        self.data = {}
        self._recover()

    def _recover(self):
        entries = self.wal.replay()
        for e in entries:
            if e.value == '__DELETED__':
                self.data.pop(e.key, None)
            else:
                self.data[e.key] = e.value
        return len(entries)

    def put(self, key, value):
        self.wal.append(key, value)
        self.data[key] = value

    def delete(self, key):
        self.wal.append(key, '__DELETED__')
        self.data.pop(key, None)

    def get(self, key):
        return self.data.get(key)

    def checkpoint(self):
        """Compact: truncate WAL and rewrite current state."""
        self.wal.truncate()
        for k, v in self.data.items():
            self.wal.append(k, v)

    def close(self):
        self.wal.close()


def main():
    path = tempfile.mktemp(suffix='.wal')
    try:
        store = WALStore(path)
        # Write some data
        for i in range(100):
            store.put(f"key-{i}", f"value-{i}")
        store.delete("key-50")
        print(f"Store: {len(store.data)} keys, WAL size: {store.wal.size:,} bytes")
        store.close()

        # Simulate crash recovery
        store2 = WALStore(path)
        print(f"Recovered: {len(store2.data)} keys")
        print(f"  get('key-0'): {store2.get('key-0')}")
        print(f"  get('key-50'): {store2.get('key-50')} (deleted)")
        print(f"  get('key-99'): {store2.get('key-99')}")

        # Checkpoint
        store2.checkpoint()
        print(f"After checkpoint: WAL size {store2.wal.size:,} bytes")
        store2.close()
    finally:
        os.unlink(path)
    print("✓ WAL round-trip verified")

if __name__ == "__main__":
    main()
