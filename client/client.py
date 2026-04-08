import socket

class KVClient:
    def __init__(self, host="127.0.0.1", port=7777):
        self._sock = socket.create_connection((host, port))
        self._file = self._sock.makefile("rw", buffering=1)  # line-buffered

    def _send(self, line: str) -> str:
        self._file.write(line + "\n")
        self._file.flush()
        return self._file.readline().rstrip("\n")

    def put(self, key: str, value: str) -> None:
        resp = self._send(f"PUT {key} {value}")
        if not resp.startswith("OK"):
            raise RuntimeError(f"PUT failed: {resp}")

    def get(self, key: str):
        resp = self._send(f"GET {key}")
        if resp == "NOT_FOUND":
            return None
        if resp.startswith("OK "):
            return resp[3:]
        raise RuntimeError(f"GET failed: {resp}")

    def delete(self, key: str) -> None:
        resp = self._send(f"DEL {key}")
        if not resp.startswith("OK"):
            raise RuntimeError(f"DEL failed: {resp}")

    def close(self):
        self._send("QUIT")
        self._sock.close()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()