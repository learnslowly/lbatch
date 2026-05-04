from __future__ import annotations

from dataclasses import dataclass

from .errors import ParseError


@dataclass(frozen=True)
class ArrayExpansion:
    task_ids: list[int]
    concurrency_limit: int | None
    original_spec: str

    @property
    def count(self) -> int:
        return len(self.task_ids)

    @property
    def minimum(self) -> int | None:
        return min(self.task_ids) if self.task_ids else None

    @property
    def maximum(self) -> int | None:
        return max(self.task_ids) if self.task_ids else None

    @property
    def step(self) -> int | None:
        if len(self.task_ids) < 2:
            return None
        diffs = {b - a for a, b in zip(self.task_ids, self.task_ids[1:])}
        return diffs.pop() if len(diffs) == 1 else None


def parse_array_spec(spec: str) -> ArrayExpansion:
    if not spec:
        raise ParseError("empty --array specification")
    base = spec
    concurrency = None
    if "%" in spec:
        base, concurrency_text = spec.rsplit("%", 1)
        if not concurrency_text.isdigit() or int(concurrency_text) <= 0:
            raise ParseError(f"invalid array concurrency limit: {spec}")
        concurrency = int(concurrency_text)
    seen: set[int] = set()
    tasks: list[int] = []
    for part in base.split(","):
        part = part.strip()
        if not part:
            raise ParseError(f"invalid array specification: {spec}")
        step = 1
        if ":" in part:
            part, step_text = part.split(":", 1)
            if not step_text.isdigit() or int(step_text) <= 0:
                raise ParseError(f"invalid array step: {spec}")
            step = int(step_text)
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            if not start_text.isdigit() or not end_text.isdigit():
                raise ParseError(f"invalid array range: {spec}")
            start, end = int(start_text), int(end_text)
            if end < start:
                raise ParseError(f"array range end before start: {spec}")
            values = range(start, end + 1, step)
        else:
            if not part.isdigit():
                raise ParseError(f"invalid array task id: {spec}")
            values = [int(part)]
        for value in values:
            if value not in seen:
                seen.add(value)
                tasks.append(value)
    if not tasks:
        raise ParseError(f"array specification produced no tasks: {spec}")
    return ArrayExpansion(tasks, concurrency, spec)
