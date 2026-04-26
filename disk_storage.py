import time


class DiskStorage:
    def __init__(self, algorithm="linear"):
        self.algorithm = algorithm
        self.store = []  # list of (key, value) tuples

    def _find_index(self, key):
        for i, (k, v) in enumerate(self.store):
            if k == key:
                return i
        return -1

    # ─── SET ──────────────────────────────────────────────────────────────────

    def set(self, key, value):
        start = time.perf_counter()
        steps = 0

        idx = self._find_index(key)
        steps += len(self.store)  # scan cost

        if idx >= 0:
            self.store[idx] = (key, value)
        else:
            self.store.append((key, value))

        # Keep sorted for binary / merge / quick lookups
        if self.algorithm in ("binary", "merge", "quick"):
            self.store = self._sort(self.store)
            steps += int(len(self.store) * (len(self.store).bit_length()))

        elapsed = (time.perf_counter() - start) * 1000
        return {
            "status": "OK",
            "time_ms": round(elapsed, 4),
            "steps": steps,
            "store_size": len(self.store),
            "algorithm": self.algorithm
        }

    # ─── GET ──────────────────────────────────────────────────────────────────

    def get(self, key):
        start = time.perf_counter()
        result = None
        steps = 0

        if self.algorithm == "linear":
            result, steps = self._linear_search(key)
        elif self.algorithm == "binary":
            result, steps = self._binary_search(key)
        elif self.algorithm == "merge":
            sorted_data = self._merge_sort(list(self.store))
            result, steps = self._binary_search_on(key, sorted_data)
            steps += int(len(self.store) * (len(self.store).bit_length()))
        elif self.algorithm == "quick":
            sorted_data = self._quick_sort(list(self.store))
            result, steps = self._binary_search_on(key, sorted_data)
            steps += int(len(self.store) * (len(self.store).bit_length()))

        elapsed = (time.perf_counter() - start) * 1000
        return {
            "status": "OK" if result is not None else "nil",
            "value": result,
            "time_ms": round(elapsed, 4),
            "steps": steps,
            "store_size": len(self.store),
            "algorithm": self.algorithm
        }

    # ─── DELETE ───────────────────────────────────────────────────────────────

    def delete(self, key):
        start = time.perf_counter()
        idx = self._find_index(key)
        steps = idx + 1 if idx >= 0 else len(self.store)
        deleted = False
        if idx >= 0:
            self.store.pop(idx)
            deleted = True
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "status": "OK" if deleted else "nil",
            "deleted": deleted,
            "time_ms": round(elapsed, 4),
            "steps": steps,
            "store_size": len(self.store),
            "algorithm": self.algorithm
        }

    # ─── SEARCH ALGORITHMS ────────────────────────────────────────────────────

    def _linear_search(self, key):
        for i, (k, v) in enumerate(self.store):
            if k == key:
                return v, i + 1
        return None, len(self.store)

    def _binary_search(self, key):
        return self._binary_search_on(key, self.store)

    def _binary_search_on(self, key, data):
        lo, hi = 0, len(data) - 1
        steps = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            steps += 1
            if data[mid][0] == key:
                return data[mid][1], steps
            elif data[mid][0] < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return None, steps

    # ─── SORT ALGORITHMS ──────────────────────────────────────────────────────

    def _sort(self, data):
        if self.algorithm == "merge":
            return self._merge_sort(data)
        elif self.algorithm == "quick":
            return self._quick_sort(data)
        else:
            return sorted(data, key=lambda x: x[0])

    def _merge_sort(self, data):
        if len(data) <= 1:
            return data
        mid = len(data) // 2
        left = self._merge_sort(data[:mid])
        right = self._merge_sort(data[mid:])
        return self._merge(left, right)

    def _merge(self, left, right):
        result = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i][0] <= right[j][0]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        result.extend(left[i:])
        result.extend(right[j:])
        return result

    def _quick_sort(self, data):
        if len(data) <= 1:
            return data
        pivot = data[len(data) // 2][0]
        left = [x for x in data if x[0] < pivot]
        middle = [x for x in data if x[0] == pivot]
        right = [x for x in data if x[0] > pivot]
        return self._quick_sort(left) + middle + self._quick_sort(right)

    # ─── STATE ────────────────────────────────────────────────────────────────

    def get_state(self):
        total_size = sum(len(str(k).encode()) + len(str(v).encode()) for k, v in self.store)
        return {
            "items": {k: v for k, v in self.store},
            "algorithm": self.algorithm,
            "count": len(self.store),
            "total_memory": total_size,
            "store_order": [k for k, v in self.store]
        }

    def flush(self):
        self.store = []

    def set_algorithm(self, algo):
        self.algorithm = algo
        self.store = []
