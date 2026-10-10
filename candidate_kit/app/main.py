"""
Employee Attendance & Analytics API - Complete Production Solution
Tech Stack: Python 3.11+, FastAPI, MongoDB (PyMongo)
Contract: openapi.yaml & DATA_MODEL.md
All endpoints, business rules (R1-R10), concurrency safety, and aggregation pipelines included.
"""

import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path, status
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()

# --------------------------------------------------------------------------- #
# Database Connection & Timezone Setup
# --------------------------------------------------------------------------- #
# Timezone IST (UTC+05:30) per Rule R1
IST = timezone(timedelta(hours=5, minutes=30))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

client = MongoClient(MONGO_URI, tz_aware=True)
db = client[MONGO_DB]


# --------------------------------------------------------------------------- #
# Application Lifespan (Index Creation at Startup)
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup lifecycle: Database indexes ko startup par create karta hai.
    Ye indexes 100k records par query ko IXSCAN execute karne mein madad karte hain.
    """
    try:
        # 1. Employees unique index on emp_code
        db.employees.create_index([("emp_code", ASCENDING)], unique=True)
        db.employees.create_index([("department", ASCENDING)])
        db.employees.create_index([("joined_on", ASCENDING)])

        # 2. Attendance Logs compound unique index on (emp_code, date)
        # Concurrency race conditions ko 409 conflict handle karne ke liye
        db.attendance_logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True)
        # Search & sorting index for GET /attendance
        db.attendance_logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)])
        db.attendance_logs.create_index([("status", ASCENDING), ("date", ASCENDING)])
        db.attendance_logs.create_index([("emp_code", ASCENDING), ("punch_in", ASCENDING)])
    except Exception as e:
        print(f"Index creation warning: {e}")
    yield


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    lifespan=lifespan
)


# --------------------------------------------------------------------------- #
# Mathematical & Rule Helper Functions (R1 - R8)
# --------------------------------------------------------------------------- #
def round_half_up(val: float, decimals: int = 2) -> float:
    """R8 Rounding: Half-up rounding implement karta hai (Banker's rounding avoid karne ke liye)."""
    if val is None:
        return None
    d = Decimal(str(val))
    fmt = "0." + "0" * decimals if decimals > 0 else "0"
    return float(d.quantize(Decimal(fmt), rounding=ROUND_HALF_UP))


def truncate_to_seconds(dt: datetime) -> datetime:
    """R1: Calculations aur storage se pehle milliseconds ko truncate karta hai."""
    if dt is None:
        return None
    return dt.replace(microsecond=0)


def ms_to_datetime(epoch_ms: Optional[int]) -> Optional[datetime]:
    """Epoch milliseconds ko UTC datetime mein convert karta hai."""
    if epoch_ms is None:
        return None
    # Truncate to whole seconds per R1
    epoch_sec = epoch_ms // 1000
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc)


def datetime_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """UTC BSON Datetime ko API output ke liye epoch milliseconds mein convert karta hai."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_time_str(t_str: str) -> tuple[int, int]:
    """HH:MM string ko hour aur minute integers mein parse karta hai."""
    h, m = map(int, t_str.split(":"))
    return h, m


def get_attendance_date(punch_in_utc: datetime, shift_start_str: str, shift_end_str: str) -> str:
    """
    R1: Attendance calendar date (YYYY-MM-DD) determine karta hai in IST.
    Overnight shifts: agar punch_in IST time < shift_end IST time, toh attendance previous date ki hoti hai.
    """
    punch_ist = punch_in_utc.astimezone(IST)
    start_h, start_m = parse_time_str(shift_start_str)
    end_h, end_m = parse_time_str(shift_end_str)

    # Check overnight shift: shift_end <= shift_start
    is_overnight = (end_h, end_m) <= (start_h, start_m)

    punch_date = punch_ist.date()
    punch_time = punch_ist.time()

    if is_overnight:
        shift_end_time = time(end_h, end_m, 0)
        if punch_time < shift_end_time:
            # Punch belongs to previous calendar day
            punch_date = punch_date - timedelta(days=1)

    return punch_date.isoformat()


def compute_late_minutes(punch_in_utc: datetime, shift_start_str: str, att_date_str: str) -> int:
    """
    R2 Late Rule: Grace period 10 minutes.
    Late tabhi hota hai jab punch_in strictly > shift_start + 10:00 ho.
    Agar late hai, toh shift_start se floor minutes count hote hain.
    """
    punch_in_sec = truncate_to_seconds(punch_in_utc)
    punch_ist = punch_in_sec.astimezone(IST)

    h, m = parse_time_str(shift_start_str)
    d = date.fromisoformat(att_date_str)
    shift_start_dt = datetime.combine(d, time(h, m, 0), tzinfo=IST)

    diff_sec = (punch_ist - shift_start_dt).total_seconds()
    # Grace period: strictly > 600 seconds (10 minutes)
    if diff_sec > 600:
        return int(diff_sec // 60)
    return 0


def compute_overtime_minutes(punch_out_utc: datetime, shift_start_str: str, shift_end_str: str, att_date_str: str) -> int:
    """
    R3 Overtime Rule: Shift end se punch_out tak ke floor minutes.
    Count tabhi hoga jab overtime >= 30 minutes ho, warna 0.
    """
    punch_out_sec = truncate_to_seconds(punch_out_utc)
    punch_ist = punch_out_sec.astimezone(IST)

    start_h, start_m = parse_time_str(shift_start_str)
    end_h, end_m = parse_time_str(shift_end_str)
    is_overnight = (end_h, end_m) <= (start_h, start_m)

    d = date.fromisoformat(att_date_str)
    # Overnight shift ends on the next calendar day
    end_date = d + timedelta(days=1) if is_overnight else d
    shift_end_dt = datetime.combine(end_date, time(end_h, end_m, 0), tzinfo=IST)

    diff_sec = (punch_ist - shift_end_dt).total_seconds()
    if diff_sec >= 1800:  # 30 minutes minimum
        return int(diff_sec // 60)
    return 0


def compute_work_hours(punch_in_utc: datetime, punch_out_utc: datetime) -> Optional[float]:
    """R4 Work Hours: (punch_out - punch_in) in seconds / 3600, rounded to 2 decimals half-up."""
    if not punch_in_utc or not punch_out_utc:
        return None
    in_sec = truncate_to_seconds(punch_in_utc)
    out_sec = truncate_to_seconds(punch_out_utc)
    seconds = (out_sec - in_sec).total_seconds()
    hours = seconds / 3600.0
    return round_half_up(hours, 2)


def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5 Half Day: Rounded work_hours < 4.50 hone par half_day = True."""
    if work_hours is None:
        return False
    return work_hours < 4.50


# --------------------------------------------------------------------------- #
# Pydantic Schemas for Request & Validation
# --------------------------------------------------------------------------- #
class EmployeeCreate(BaseModel):
    emp_code: str = Field(..., pattern=r"^EMP\d{4,6}$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(..., min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    shift_end: str = Field(default="18:30", pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    joined_on: str

    @field_validator("shift_end")
    @classmethod
    def validate_shift_times(cls, v, info):
        start = info.data.get("shift_start")
        if start and v == start:
            raise ValueError("shift_start must differ from shift_end")
        return v


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    status: str = Field(default="PRESENT")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        if v not in ["PRESENT", "WFH", "ON_DUTY"]:
            raise ValueError("Status must be PRESENT, WFH, or ON_DUTY")
        return v


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[int] = Field(None, ge=100000000000, le=4102444800000)


class RegularizeRequest(BaseModel):
    status: Optional[str] = None
    punch_in: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    punch_out: Optional[int] = Field(None, ge=100000000000, le=4102444800000)
    reason: str = Field(..., min_length=5, max_length=200)
    regularized_by: str = Field(..., min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# Response Serializer Helpers
# --------------------------------------------------------------------------- #
def serialize_employee(emp: dict) -> dict:
    return {
        "emp_code": emp["emp_code"],
        "name": emp["name"],
        "email": emp["email"],
        "department": emp["department"],
        "shift_start": emp["shift_start"],
        "shift_end": emp["shift_end"],
        "joined_on": emp["joined_on"],
        "created_at": datetime_to_ms(emp.get("created_at")),
    }


def serialize_history(history_list: list) -> list:
    out = []
    for h in history_list or []:
        entry = {
            "at": datetime_to_ms(h["at"]),
            "by": h["by"],
            "reason": h["reason"],
            "changes": {}
        }
        for k, v in h.get("changes", {}).items():
            entry["changes"][k] = {
                "from": datetime_to_ms(v["from"]) if k in ["punch_in", "punch_out"] else v["from"],
                "to": datetime_to_ms(v["to"]) if k in ["punch_in", "punch_out"] else v["to"]
            }
        out.append(entry)
    return out


def serialize_attendance(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": datetime_to_ms(doc.get("punch_in")),
        "punch_out": datetime_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes", 0),
        "overtime_minutes": doc.get("overtime_minutes", 0),
        "half_day": doc.get("half_day", False),
        "history": serialize_history(doc.get("history", [])),
    }


# --------------------------------------------------------------------------- #
# System Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["system"])
def health():
    """MongoDB readiness probe: answers 200 if MongoDB pings, else 503."""
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database connection unavailable")


# --------------------------------------------------------------------------- #
# Employee Endpoints
# --------------------------------------------------------------------------- #
@app.post("/employees", status_code=status.HTTP_201_CREATED, tags=["employees"])
def create_employee(body: EmployeeCreate):
    """Creates an employee. Unique emp_code is enforced via MongoDB index (race-safe)."""
    doc = body.model_dump()
    doc["created_at"] = truncate_to_seconds(datetime.now(timezone.utc))

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail=f"emp_code {body.emp_code} already exists")

    return serialize_employee(doc)


@app.get("/employees", tags=["employees"])
def list_employees(
    department: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List employees paginated and sorted by emp_code ASC."""
    q = {}
    if department:
        q["department"] = department

    total = db.employees.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.employees.find(q).sort("emp_code", ASCENDING).skip(skip).limit(page_size)
    items = [serialize_employee(emp) for emp in cursor]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# Attendance Endpoints (Punch-In, Punch-Out, List, Regularize)
# --------------------------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=status.HTTP_201_CREATED, tags=["attendance"])
def punch_in(body: PunchInRequest):
    """Records punch-in. Atomic insertion prevents duplicate punch-in races."""
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    punch_utc = ms_to_datetime(body.punched_at) if body.punched_at else truncate_to_seconds(datetime.now(timezone.utc))
    att_date = get_attendance_date(punch_utc, emp["shift_start"], emp["shift_end"])
    late_mins = compute_late_minutes(punch_utc, emp["shift_start"], att_date)

    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": punch_utc,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late_mins,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail=f"Already punched in for date {att_date}")

    return serialize_attendance(doc)


@app.post("/attendance/punch-out", status_code=status.HTTP_200_OK, tags=["attendance"])
def punch_out(body: PunchOutRequest):
    """
    Closes employee's open attendance record whose punch_in <= punched_at.
    Handles concurrency via atomic find_one_and_update.
    """
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    punch_out_utc = ms_to_datetime(body.punched_at) if body.punched_at else truncate_to_seconds(datetime.now(timezone.utc))

    # Find the latest open record for this employee
    record = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": punch_out_utc}},
        sort=[("punch_in", DESCENDING)]
    )

    if not record or not record.get("punch_in"):
        raise HTTPException(status_code=404, detail="No matching punch-in record found")

    punch_in_utc = record["punch_in"]
    if punch_out_utc <= punch_in_utc:
        raise HTTPException(status_code=422, detail="punch_out must be strictly greater than punch_in")

    if (punch_out_utc - punch_in_utc).total_seconds() > 86400:
        raise HTTPException(status_code=422, detail="punch_out cannot be more than 24 hours after punch_in")

    if record.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="Record is already punched out")

    # Derived fields computation
    att_date = record["date"]
    work_hrs = compute_work_hours(punch_in_utc, punch_out_utc)
    ot_mins = compute_overtime_minutes(punch_out_utc, emp["shift_start"], emp["shift_end"], att_date)
    half_d = compute_half_day(work_hrs)

    # Atomic update to guarantee single winner under simultaneous punch-outs
    updated = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {
            "punch_out": punch_out_utc,
            "work_hours": work_hrs,
            "overtime_minutes": ot_mins,
            "half_day": half_d,
        }},
        return_document=True
    )

    if not updated:
        raise HTTPException(status_code=409, detail="Record was already punched out concurrently")

    return serialize_attendance(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Lists attendance records efficiently from MongoDB index."""
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from cannot be greater than date_to")

    q = {}
    if emp_code:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status:
        q["status"] = status

    total = db.attendance_logs.count_documents(q)
    skip = (page - 1) * page_size
    cursor = db.attendance_logs.find(q).sort([("date", DESCENDING), ("emp_code", ASCENDING)]).skip(skip).limit(page_size)

    items = [serialize_attendance(doc) for doc in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(
    emp_code: str = Path(...),
    date: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    body: RegularizeRequest = ...
):
    """
    Manual regularisation: Updates fields, recomputes derived metrics and appends audit log.
    Concurrency safe with optimistic version checking.
    """
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if not record:
        raise HTTPException(status_code=404, detail="Attendance record not found for this date")

    new_status = body.status if body.status is not None else record["status"]
    new_in = ms_to_datetime(body.punch_in) if body.punch_in is not None else record.get("punch_in")
    new_out = ms_to_datetime(body.punch_out) if body.punch_out is not None else record.get("punch_out")

    # Validation rules
    if new_status in ["ABSENT", "LEAVE"]:
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(status_code=422, detail="Punch times cannot be supplied for ABSENT or LEAVE status")
        new_in = None
        new_out = None
    else:
        if not new_in:
            raise HTTPException(status_code=422, detail="Presence status requires punch_in time")

    if new_in:
        # Check that punch_in stays on the record's attendance date
        computed_date = get_attendance_date(new_in, emp["shift_start"], emp["shift_end"])
        if computed_date != date:
            raise HTTPException(status_code=422, detail="punch_in must stay on the record's attendance date")

    if new_in and new_out:
        if new_out <= new_in:
            raise HTTPException(status_code=422, detail="punch_out must be after punch_in")
        if (new_out - new_in).total_seconds() > 86400:
            raise HTTPException(status_code=422, detail="punch_out must be within 24 hours of punch_in")

    # Recompute derived values
    if new_status in ["ABSENT", "LEAVE"]:
        work_hrs = None
        late_mins = 0
        ot_mins = 0
        half_d = False
    else:
        late_mins = compute_late_minutes(new_in, emp["shift_start"], date)
        if new_out:
            work_hrs = compute_work_hours(new_in, new_out)
            ot_mins = compute_overtime_minutes(new_out, emp["shift_start"], emp["shift_end"], date)
            half_d = compute_half_day(work_hrs)
        else:
            work_hrs = None
            ot_mins = 0
            half_d = False

    # Check which fields actually changed
    candidate_changes = {
        "status": (record.get("status"), new_status),
        "punch_in": (record.get("punch_in"), new_in),
        "punch_out": (record.get("punch_out"), new_out),
        "work_hours": (record.get("work_hours"), work_hrs),
        "late_minutes": (record.get("late_minutes", 0), late_mins),
        "overtime_minutes": (record.get("overtime_minutes", 0), ot_mins),
        "half_day": (record.get("half_day", False), half_d),
    }

    changes_dict = {}
    for field, (old_v, new_v) in candidate_changes.items():
        if old_v != new_v:
            changes_dict[field] = {"from": old_v, "to": new_v}

    if not changes_dict:
        raise HTTPException(status_code=422, detail="Request changed no fields")

    # Prepare audit log history entry
    now_utc = truncate_to_seconds(datetime.now(timezone.utc))
    history_entry = {
        "at": now_utc,
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes_dict
    }

    # Perform atomic update
    update_result = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "history": record.get("history", [])},
        {
            "$set": {
                "status": new_status,
                "punch_in": new_in,
                "punch_out": new_out,
                "work_hours": work_hrs,
                "late_minutes": late_mins,
                "overtime_minutes": ot_mins,
                "half_day": half_d,
            },
            "$push": {"history": history_entry}
        },
        return_document=True
    )

    if not update_result:
        raise HTTPException(status_code=409, detail="Concurrent regularization conflict")

    return serialize_attendance(update_result)


# --------------------------------------------------------------------------- #
# Part B: Analytics Endpoints (MongoDB Aggregations)
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(
    emp_code: str = Path(...),
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
):
    """Monthly attendance summary for one employee using MongoDB aggregation."""
    emp = db.employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    year, m = map(int, month.split("-"))
    # Working days in month (Mon-Fri) taking joined_on into account per R7
    start_date = date(year, m, 1)
    end_date = date(year + 1, 1, 1) - timedelta(days=1) if m == 12 else date(year, m + 1, 1) - timedelta(days=1)

    joined_dt = date.fromisoformat(emp["joined_on"])
    effective_start = max(start_date, joined_dt)

    working_days = 0
    if effective_start <= end_date:
        cur = effective_start
        while cur <= end_date:
            if cur.weekday() < 5:  # Monday to Friday
                working_days += 1
            cur += timedelta(days=1)

    date_regex = f"^{month}-\\d{{2}}$"

    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$regex": date_regex}}},
        {"$addFields": {
            "date_obj": {"$dateFromString": {"dateString": "$date"}},
        }},
        {"$addFields": {
            "dayOfWeek": {"$dayOfWeek": "$date_obj"} # 1: Sun, 7: Sat
        }},
        {"$group": {
            "_id": None,
            "present_days": {
                "$sum": {
                    "$cond": [
                        {
                            "$and": [
                                {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$not": {"$in": ["$dayOfWeek", [1, 7]]}} # Mon-Fri only
                            ]
                        },
                        {"$cond": ["$half_day", 0.5, 1.0]},
                        0.0
                    ]
                }
            },
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": ["$late_minutes", 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": "$late_minutes"},
            "total_overtime_minutes": {"$sum": "$overtime_minutes"}
        }}
    ]

    agg = list(db.attendance_logs.aggregate(pipeline))
    res = agg[0] if agg else {}

    present_days = res.get("present_days", 0.0)
    leave_days = res.get("leave_days", 0)
    late_count = res.get("late_count", 0)
    total_late = res.get("total_late_minutes", 0)
    total_ot = res.get("total_overtime_minutes", 0)

    attendance_pct = None
    if working_days > 0:
        attendance_pct = round_half_up((present_days / working_days) * 100, 2)

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late,
        "total_overtime_minutes": total_ot,
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = Query(None)
):
    """
    Department monthly summary cross-collection aggregation:
    Includes employees with zero logs per R9, excludes those joining later.
    """
    year, m = map(int, month.split("-"))
    end_of_month = (date(year + 1, 1, 1) - timedelta(days=1) if m == 12 else date(year, m + 1, 1) - timedelta(days=1)).isoformat()

    match_emp = {"joined_on": {"$lte": end_of_month}}
    if department:
        match_emp["department"] = department

    date_regex = f"^{month}-\\d{{2}}$"

    pipeline = [
        {"$match": match_emp},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"code": "$emp_code"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$emp_code", "$$code"]},
                    "date": {"$regex": date_regex}
                }},
                {"$addFields": {
                    "date_obj": {"$dateFromString": {"dateString": "$date"}},
                }},
                {"$addFields": {
                    "dayOfWeek": {"$dayOfWeek": "$date_obj"}
                }}
            ],
            "as": "logs"
        }},
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {
                "department": "$department",
                "emp_code": "$emp_code"
            },
            "logs": {"$push": "$logs"}
        }},
        {"$project": {
            "department": "$_id.department",
            "emp_code": "$_id.emp_code",
            "logs": {
                "$filter": {
                    "input": "$logs",
                    "as": "l",
                    "cond": {"$ne": ["$$l", {}]}
                }
            }
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "all_logs": {"$push": "$logs"}
        }},
        {"$project": {
            "department": "$_id",
            "headcount": 1,
            "flat_logs": {
                "$reduce": {
                    "input": "$all_logs",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
            }
        }},
        {"$unwind": {"path": "$flat_logs", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {
                "department": "$department",
                "headcount": "$headcount"
            },
            "present_days": {
                "$sum": {
                    "$cond": [
                        {
                            "$and": [
                                {"$in": ["$flat_logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$not": {"$in": ["$flat_logs.dayOfWeek", [1, 7]]}}
                            ]
                        },
                        {"$cond": ["$flat_logs.half_day", 0.5, 1.0]},
                        0.0
                    ]
                }
            },
            "total_work_hours": {
                "$sum": {
                    "$cond": [
                        {
                            "$and": [
                                {"$in": ["$flat_logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$ne": ["$flat_logs.work_hours", None]}
                            ]
                        },
                        "$flat_logs.work_hours",
                        0.0
                    ]
                }
            },
            "work_hours_count": {
                "$sum": {
                    "$cond": [
                        {
                            "$and": [
                                {"$in": ["$flat_logs.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                {"$ne": ["$flat_logs.work_hours", None]}
                            ]
                        },
                        1,
                        0
                    ]
                }
            },
            "late_count": {
                "$sum": {"$cond": [{"$gt": ["$flat_logs.late_minutes", 0]}, 1, 0]}
            },
            "total_late_minutes": {
                "$sum": {"$ifNull": ["$flat_logs.late_minutes", 0]}
            },
            "leave_count": {
                "$sum": {"$cond": [{"$eq": ["$flat_logs.status", "LEAVE"]}, 1, 0]}
            },
            "on_duty_count": {
                "$sum": {"$cond": [{"$eq": ["$flat_logs.status", "ON_DUTY"]}, 1, 0]}
            }
        }},
        {"$sort": {"_id.department": ASCENDING}}
    ]

    results = list(db.employees.aggregate(pipeline))
    items = []

    for r in results:
        dept = r["_id"]["department"]
        headcount = r["_id"]["headcount"]
        wh_count = r["work_hours_count"]
        avg_wh = round_half_up(r["total_work_hours"] / wh_count, 2) if wh_count > 0 else None

        items.append({
            "department": dept,
            "headcount": headcount,
            "present_days": r["present_days"],
            "avg_work_hours": avg_wh,
            "late_count": r["late_count"],
            "total_late_minutes": r["total_late_minutes"],
            "leave_count": r["leave_count"],
            "on_duty_count": r["on_duty_count"]
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = Query(None)
):
    """
    Ranks late employees using Standard Competition Ranking (1, 2, 2, 4).
    Limit applies strictly AFTER ranking. Ties at cutoff are included.
    """
    date_regex = f"^{month}-\\d{{2}}$"

    match_log = {
        "date": {"$regex": date_regex},
        "late_minutes": {"$gt": 0}
    }

    pipeline = [
        {"$match": match_log},
        {"$group": {
            "_id": "$emp_code",
            "total_late_minutes": {"$sum": "$late_minutes"},
            "late_count": {"$sum": 1}
        }},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "as": "emp"
        }},
        {"$unwind": "$emp"},
    ]

    if department:
        pipeline.append({"$match": {"emp.department": department}})

    pipeline.extend([
        {"$project": {
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1
        }},
        {"$sort": {"total_late_minutes": DESCENDING, "emp_code": ASCENDING}}
    ])

    rows = list(db.attendance_logs.aggregate(pipeline))

    # Standard Competition Ranking in Python (1, 2, 2, 4)
    ranked_items = []
    current_rank = 1
    for i, row in enumerate(rows):
        if i > 0:
            if row["total_late_minutes"] == rows[i - 1]["total_late_minutes"]:
                # Share same rank on tie
                rank = ranked_items[-1]["rank"]
            else:
                rank = i + 1
        else:
            rank = 1

        ranked_items.append({
            "rank": rank,
            "emp_code": row["emp_code"],
            "name": row["name"],
            "department": row["department"],
            "total_late_minutes": row["total_late_minutes"],
            "late_count": row["late_count"]
        })

    # Filter where rank <= limit
    filtered_items = [item for item in ranked_items if item["rank"] <= limit]

    return {"month": month, "items": filtered_items}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(
    department: str = Path(...),
    from_date: str = Query(..., alias="from", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    to_date: str = Query(..., alias="to", pattern=r"^\d{4}-\d{2}-\d{2}$")
):
    """
    Daily attendance trend with gap filling and 7-day moving average.
    Returns exactly one row per calendar day.
    """
    d_from = date.fromisoformat(from_date)
    d_to = date.fromisoformat(to_date)

    if d_to < d_from:
        raise HTTPException(status_code=422, detail="to date cannot be earlier than from date")

    days_diff = (d_to - d_from).days + 1
    if days_diff > 92:
        raise HTTPException(status_code=422, detail="Date range cannot exceed 92 days")

    # Verify department exists
    dept_exists = db.employees.find_one({"department": department})
    if not dept_exists:
        raise HTTPException(status_code=404, detail="Department not found")

    pipeline = [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"code": "$emp_code"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$emp_code", "$$code"]},
                    "date": {"$gte": from_date, "$lte": to_date}
                }}
            ],
            "as": "logs"
        }},
        {"$project": {"emp_code": 1, "joined_on": 1, "logs": 1}}
    ]

    emp_data = list(db.employees.aggregate(pipeline))

    # Pre-map daily aggregation for attendance logs
    logs_by_date = {}
    for emp in emp_data:
        for l in emp.get("logs", []):
            d_str = l["date"]
            if d_str not in logs_by_date:
                logs_by_date[d_str] = []
            logs_by_date[d_str].append(l)

    items = []
    rates_history = []
    cur = d_from

    while cur <= d_to:
        cur_str = cur.isoformat()
        is_working_day = cur.weekday() < 5

        # Headcount per R9: joined_on <= cur
        headcount = sum(1 for e in emp_data if e["joined_on"] <= cur_str)

        day_logs = logs_by_date.get(cur_str, [])
        present_count = 0.0
        late_count = 0

        for l in day_logs:
            if l.get("status") in ["PRESENT", "WFH", "ON_DUTY"]:
                present_count += 0.5 if l.get("half_day") else 1.0
            if (l.get("late_minutes") or 0) > 0:
                late_count += 1

        attendance_rate = None
        if is_working_day and headcount > 0:
            attendance_rate = round_half_up(present_count / headcount, 4)

        rates_history.append(attendance_rate)

        # 7-day moving average over preceding non-null rates within range
        window = rates_history[-7:]
        valid_rates = [r for r in window if r is not None]
        moving_avg = round_half_up(sum(valid_rates) / len(valid_rates), 4) if valid_rates else None

        items.append({
            "date": cur_str,
            "is_working_day": is_working_day,
            "headcount": headcount,
            "present_count": present_count,
            "late_count": late_count,
            "attendance_rate": attendance_rate,
            "moving_avg_7d": moving_avg
        })
        cur += timedelta(days=1)

    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# Part C: Admin Explain Endpoint
# --------------------------------------------------------------------------- #
@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: str = Path(..., regex="^(attendance_list|employee_monthly|department_summary|late_leaderboard|department_trend)$"),
    emp_code: Optional[str] = Query(None),
    month: Optional[str] = Query(None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """
    Returns raw MongoDB explain executionStats for verifying IXSCAN usage.
    """
    if endpoint == "attendance_list":
        q = {}
        if emp_code:
            q["emp_code"] = emp_code
        if date_from or date_to:
            q["date"] = {}
            if date_from:
                q["date"]["$gte"] = date_from
            if date_to:
                q["date"]["$lte"] = date_to
        if status:
            q["status"] = status
        skip = (page - 1) * page_size
        explain = db.command(
            "explain",
            {
                "find": "attendance_logs",
                "filter": q,
                "sort": {"date": -1, "emp_code": 1},
                "skip": skip,
                "limit": page_size
            },
            verbosity="executionStats"
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "employee_monthly":
        if not emp_code or not month:
            raise HTTPException(status_code=422, detail="emp_code and month are required")
        date_regex = f"^{month}-\\d{{2}}$"
        explain = db.command(
            "explain",
            {
                "aggregate": "attendance_logs",
                "pipeline": [{"$match": {"emp_code": emp_code, "date": {"$regex": date_regex}}}],
                "cursor": {}
            },
            verbosity="executionStats"
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(status_code=422, detail="month is required")
        explain = db.command(
            "explain",
            {
                "aggregate": "employees",
                "pipeline": [{"$match": {"joined_on": {"$lte": f"{month}-31"}}}],
                "cursor": {}
            },
            verbosity="executionStats"
        )
        return {"endpoint": endpoint, "collection": "employees", "explain": explain}

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(status_code=422, detail="month is required")
        date_regex = f"^{month}-\\d{{2}}$"
        explain = db.command(
            "explain",
            {
                "aggregate": "attendance_logs",
                "pipeline": [{"$match": {"date": {"$regex": date_regex}, "late_minutes": {"$gt": 0}}}],
                "cursor": {}
            },
            verbosity="executionStats"
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "department_trend":
        if not department or not from_date or not to_date:
            raise HTTPException(status_code=422, detail="department, from, and to are required")
        explain = db.command(
            "explain",
            {
                "aggregate": "employees",
                "pipeline": [{"$match": {"department": department}}],
                "cursor": {}
            },
            verbosity="executionStats"
        )
        return {"endpoint": endpoint, "collection": "employees", "explain": explain}

    raise HTTPException(status_code=422, detail="Unknown explain endpoint")