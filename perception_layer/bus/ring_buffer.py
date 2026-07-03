from collections import deque
from typing import TypeVar, Generic

T = TypeVar("T")


class RingBuffer(Generic[T]):
    """固定容量环形缓冲。满时覆盖最老元素（不留信息，原型接受此简化）。
    文档的完整版应分级持久化——关键事件落盘，非关键仅环形缓冲30秒后丢弃。
    原型不分级，缓冲满即覆盖——代价：高频事件可能丢失。声明此简化。
    """

    def __init__(self, capacity: int = 2048) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._buffer: deque[T] = deque(maxlen=capacity)

    def push(self, item: T) -> None:
        """推入事件。满时自动覆盖最老元素。"""
        self._buffer.append(item)

    def snapshot(self) -> list[T]:
        """返回当前缓冲内容的快照（从老到新）。"""
        return list(self._buffer)

    def latest(self, n: int = 1) -> list[T]:
        """返回最近 n 个事件。"""
        n = min(n, len(self._buffer))
        if n == 0:
            return []
        return list(self._buffer)[-n:]

    def __len__(self) -> int:
        return len(self._buffer)

    @property
    def capacity(self) -> int:
        return self._capacity

    def clear(self) -> None:
        self._buffer.clear()
