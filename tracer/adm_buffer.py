# tracer/adm_buffer.py
from collections import deque
from typing import Deque

class AdmBuffer:
    """
    Minimal de-duplication: remembers the last N lines seen to avoid double-processing
    when FTP servers resend trailing chunks, or when offsets shift slightly.
    """
    def __init__(self, max_remember: int = 100):
        self.last: Deque[str] = deque(maxlen=max_remember)

    def accept(self, line: str) -> bool:
        line = line.rstrip("\r\n")
        if not line:
            return False
        if line in self.last:
            return False
        self.last.append(line)
        return True
