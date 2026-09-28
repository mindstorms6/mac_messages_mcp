"""Bounded, keyset-paginated history reads (no writes to Messages)."""

import base64
import hashlib
import json
from datetime import datetime, timezone

APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=timezone.utc)
# Older databases store seconds; current databases store nanoseconds.
DATE_NS = (
    "(CASE WHEN ABS(m.date) < 10000000000 THEN m.date * 1000000000 ELSE m.date END)"
)


def apple_ns(dt):
    delta = dt - APPLE_EPOCH
    return ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 1000


def parse_date(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return apple_ns(dt)


class HistoryPage:
    """A cursor binds the filters and freezes the relative time window.

    The cursor is an opaque continuation token, not a credential. All decoded
    values are validated and SQL-bound. Equal timestamps use ROWID as a tie-break.
    """

    def __init__(self, hours, limit, before, after, cursor, scope):
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        self.limit = limit
        self.scope = hashlib.sha256(
            json.dumps([hours, before, after, scope], sort_keys=True).encode()
        ).hexdigest()
        self.anchor = apple_ns(datetime.now(timezone.utc))
        self.last = None
        if cursor:
            try:
                if len(cursor) > 2048:
                    raise ValueError()
                data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                if data["v"] != 1 or data["scope"] != self.scope:
                    raise ValueError()
                values = [data["anchor"], *data["last"]]
                if len(values) != 3 or any(
                    type(v) is not int or not 0 <= v < 2**63 for v in values
                ):
                    raise ValueError()
                self.anchor = data["anchor"]
                self.last = data["last"]
            except Exception as exc:
                raise ValueError(
                    "Invalid cursor or filters changed; start a new search"
                ) from exc
        self.clauses = [f"{DATE_NS} <= ?"]
        self.params = [self.anchor]
        lower = parse_date(after) if after else None
        upper = parse_date(before) if before else None
        if lower is not None and upper is not None and lower >= upper:
            raise ValueError("after must be earlier than before")
        if hours:
            cutoff = self.anchor - hours * 3600 * 1000000000
            lower = max(lower, cutoff) if lower is not None else cutoff
        for op, bound in ((">=", lower), ("<", upper)):
            if bound is not None:
                if not -(2**63) <= bound < 2**63:
                    raise ValueError("date is outside the supported range")
                self.clauses.append(f"{DATE_NS} {op} ?")
                self.params.append(bound)
        if self.last:
            self.clauses.append(f"({DATE_NS} < ? OR ({DATE_NS} = ? AND m.ROWID < ?))")
            self.params.extend([self.last[0], self.last[0], self.last[1]])

    def select(self, clauses=(), params=()):
        where = " AND ".join([*self.clauses, *clauses])
        sql = f"""SELECT m.ROWID AS ROWID, m.date, m.text, m.attributedBody,
            m.is_from_me, m.handle_id, m.cache_roomnames,
            {DATE_NS} AS date_ns
            FROM message m WHERE {where}
            ORDER BY {DATE_NS} DESC, m.ROWID DESC LIMIT ?"""
        return sql, tuple([*self.params, *params, self.limit + 1])

    def finish(self, rows):
        more = len(rows) > self.limit
        rows = rows[: self.limit]
        token = None
        if more and rows:
            row = rows[-1]
            date = int(row.get("date_ns", row["date"]))
            if abs(date) < 10000000000:
                date *= 1000000000
            token = base64.urlsafe_b64encode(
                json.dumps(
                    {
                        "v": 1,
                        "scope": self.scope,
                        "anchor": self.anchor,
                        "last": [date, row["ROWID"]],
                    },
                    separators=(",", ":"),
                ).encode()
            ).decode()
        # Put continuation first so output truncation cannot hide it. A search
        # page may match nothing; it still advances over the scanned rows.
        header = f"Scanned {len(rows)} messages; has_more={str(more).lower()}; next_cursor={token or 'null'}\n"
        return rows, header
