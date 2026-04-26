import time
import heapq
from collections import OrderedDict


class HuffmanNode:
    def __init__(self, char, freq):
        self.char = char
        self.freq = freq
        self.left = None
        self.right = None

    def __lt__(self, other):
        return self.freq < other.freq


def huffman_compress(text):
    if not text:
        return text, 1.0
    freq = {}
    for ch in text:
        freq[ch] = freq.get(ch, 0) + 1
    if len(freq) == 1:
        char = list(freq.keys())[0]
        bits = len(text)
        original = len(text) * 8
        return text, original / max(bits, 1)
    heap = [HuffmanNode(ch, f) for ch, f in freq.items()]
    heapq.heapify(heap)
    while len(heap) > 1:
        l = heapq.heappop(heap)
        r = heapq.heappop(heap)
        merged = HuffmanNode(None, l.freq + r.freq)
        merged.left = l
        merged.right = r
        heapq.heappush(heap, merged)
    codes = {}
    def build_codes(node, code=""):
        if node is None:
            return
        if node.char is not None:
            codes[node.char] = code if code else "0"
            return
        build_codes(node.left, code + "0")
        build_codes(node.right, code + "1")
    build_codes(heap[0])
    compressed_bits = sum(freq[ch] * len(codes[ch]) for ch in freq)
    original_bits = len(text) * 8
    ratio = original_bits / max(compressed_bits, 1)
    return text, ratio


class LRUCache:
    def __init__(self, capacity=10):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        evicted = None
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.capacity:
                evicted = self.cache.popitem(last=False)
        self.cache[key] = value
        return evicted

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    def get_all(self):
        return list(self.cache.keys())

    def get_order(self):
        return list(self.cache.keys())


class MiniRedis:
    def __init__(self, capacity=10):
        self.lru = LRUCache(capacity)
        self.ttl_map = {}
        self.memory_bytes = 0
        self.capacity = capacity

    def _hash_key(self, key):
        h = 0
        for ch in key:
            h = (h * 31 + ord(ch)) & 0xFFFFFFFF
        return h % 8

    def _is_expired(self, key):
        if key in self.ttl_map:
            if time.time() > self.ttl_map[key]:
                self._expire(key)
                return True
        return False

    def _expire(self, key):
        self.lru.delete(key)
        if key in self.ttl_map:
            del self.ttl_map[key]

    def set(self, key, value, ttl=None):
        start = time.perf_counter()
        _, ratio = huffman_compress(str(value))
        original_size = len(str(value).encode())
        compressed_size = int(original_size / ratio)

        evicted = self.lru.put(key, value)
        if ttl:
            self.ttl_map[key] = time.time() + ttl
        elif key in self.ttl_map:
            del self.ttl_map[key]

        elapsed = (time.perf_counter() - start) * 1000
        return {
            "status": "OK",
            "time_ms": round(elapsed, 4),
            "hash_bucket": self._hash_key(key),
            "original_size": original_size,
            "compressed_size": compressed_size,
            "compression_ratio": round(ratio, 2),
            "evicted": evicted[0] if evicted else None,
            "ttl": ttl
        }

    def get(self, key):
        start = time.perf_counter()
        if self._is_expired(key):
            elapsed = (time.perf_counter() - start) * 1000
            return {"status": "nil", "value": None, "time_ms": round(elapsed, 4), "reason": "expired"}
        value = self.lru.get(key)
        elapsed = (time.perf_counter() - start) * 1000
        if value is None:
            return {"status": "nil", "value": None, "time_ms": round(elapsed, 4)}
        return {
            "status": "OK",
            "value": value,
            "time_ms": round(elapsed, 4),
            "hash_bucket": self._hash_key(key)
        }

    def delete(self, key):
        start = time.perf_counter()
        result = self.lru.delete(key)
        if key in self.ttl_map:
            del self.ttl_map[key]
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "status": "OK" if result else "nil",
            "deleted": result,
            "time_ms": round(elapsed, 4)
        }

    def get_state(self):
        items = {}
        now = time.time()
        for k in self.lru.get_all():
            v = self.lru.cache[k]
            ttl_remaining = None
            if k in self.ttl_map:
                ttl_remaining = max(0, round(self.ttl_map[k] - now, 1))
            items[k] = {
                "value": v,
                "bucket": self._hash_key(k),
                "ttl": ttl_remaining,
                "size": len(str(v).encode())
            }
        total_mem = sum(len(str(v["value"]).encode()) for v in items.values())
        compressed_mem = sum(v["size"] for v in items.values())
        return {
            "items": items,
            "lru_order": self.lru.get_order(),
            "capacity": self.capacity,
            "count": len(items),
            "total_memory": total_mem,
            "hash_buckets": self._get_buckets(items)
        }

    def _get_buckets(self, items):
        buckets = {i: [] for i in range(8)}
        for k in items:
            b = self._hash_key(k)
            buckets[b].append(k)
        return buckets

    def flush(self):
        self.lru = LRUCache(self.capacity)
        self.ttl_map = {}
