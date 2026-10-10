# DECISIONS.md

1. **Indexes:**
We created a unique index on `employees.emp_code` and a compound unique index on `attendance_logs.(emp_code, date)` to enforce single attendance per day and prevent race conditions. We also indexed `attendance_logs.(date DESC, emp_code ASC)` for fast paginated listing and `attendance_logs.(emp_code, punch_in)` for punch-out lookups. We rejected indexing `history` array because it grows unbounded and is never queried directly.

2. **Punch-in race:**
When two identical punch-in requests arrive simultaneously, both compute the same attendance date. Both try to insert into `attendance_logs`. Thanks to our unique compound index on `(emp_code, date)`, MongoDB atomically allows exactly one write (HTTP 201), while the concurrent operation trips a `DuplicateKeyError`, caught cleanly to return HTTP 409 without check-then-insert race conditions.

3. **Ties:**
We implement Standard Competition Ranking (1, 2, 2, 4) sorted by late minutes descending and `emp_code` ascending. Limit is evaluated after ranking (`rank <= limit`), ensuring tied employees at cutoff are fairly included.

4. **Headcount:**
Department summary starts by matching from the `employees` collection filtering `joined_on <= end_of_month`. We use a `$lookup` to join logs; employees with no logs remain present via `preserveNullAndEmptyArrays: True` and are counted in headcount.

5. **One thing for 100x data:**
We would pre-aggregate daily and monthly metrics into a dedicated summary collection via background workers rather than running on-the-fly cross-collection `$lookup` aggregations.