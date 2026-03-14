import uuid
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from core.scrape import Scrape

app = FastAPI(
    title="Booking.com Reviews Parser API",
    description=(
        "Async job-based API to scrape hotel reviews from Booking.com.\n\n"
        "## Workflow\n"
        "1. **POST** `/v3/reviews/task_post` — submit one or more scraping tasks\n"
        "2. **GET** `/v3/reviews/tasks_ready` — poll for completed tasks\n"
        "3. **GET** `/v3/reviews/task_get/{id}` — retrieve results by task ID\n\n"
        "## Status codes\n"
        "| Code | Meaning |\n"
        "|------|----------|\n"
        "| 20000 | OK — task completed successfully |\n"
        "| 20100 | Task created / still in progress |\n"
        "| 40401 | Task not found |\n"
        "| 50000 | Internal error during scraping |\n\n"
        "Response envelope follows the DataForSEO v3 convention."
    ),
    version="3.0.0",
)

# ── In-memory task store ────────────────────────────────────────────────────────
tasks_store: dict[str, dict] = {}
_tasks_store_lock = threading.Lock()

# Maximum number of concurrent scrape jobs
MAX_CONCURRENT_SCRAPES = 4
_scrape_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPES)

# Task TTL: completed/failed tasks are removed after this many seconds (1 hour)
TASK_TTL_SECONDS = 3600


# ── Enums ────────────────────────────────────────────────────────────────────────
class SortBy(str, Enum):
    most_relevant = "most_relevant"
    newest_first = "newest_first"
    oldest_first = "oldest_first"
    highest_scores = "highest_scores"
    lowest_scores = "lowest_scores"


# ── Request models ───────────────────────────────────────────────────────────────
class TaskPostItem(BaseModel):
    """A single scraping task to be queued."""

    hotel_name: str = Field(
        ...,
        min_length=2,
        description="Hotel name from the booking.com URL slug, e.g. `hotel-negresco` from `booking.com/hotel/fr/hotel-negresco.html`.",
        json_schema_extra={"example": "hotel-negresco"},
    )
    country: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="ISO 3166-1 ALPHA-2 country code where the hotel is located.",
        json_schema_extra={"example": "fr"},
    )
    sort_by: SortBy = Field(
        SortBy.most_relevant,
        description="Review sort order.",
    )
    depth: int = Field(
        -1,
        description="Number of reviews to scrape. Use `-1` to scrape all available reviews.",
        json_schema_extra={"example": 50},
    )
    tag: Optional[str] = Field(
        None,
        max_length=255,
        description="User-defined label to identify this task in results. Not used by the scraper.",
        json_schema_extra={"example": "my-batch-jan-2026"},
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "hotel_name": "hotel-negresco",
                    "country": "fr",
                    "sort_by": "newest_first",
                    "depth": 50,
                    "tag": "negresco-latest",
                }
            ]
        }
    }


# ── Response models ──────────────────────────────────────────────────────────────
class TaskData(BaseModel):
    """Echo of the original request parameters stored with the task."""

    api: str = Field("reviews", description="API section identifier.")
    function: str = Field("task_post", description="Function that created the task.")
    hotel_name: str = Field(..., description="Hotel name that was requested.")
    country: str = Field(..., description="Country code that was requested.")
    sort_by: Optional[str] = Field(None, description="Sort order that was requested.")
    depth: Optional[int] = Field(None, description="Review depth that was requested.")
    tag: Optional[str] = Field(None, description="User-defined tag, if provided.")


class ReviewItem(BaseModel):
    """A single scraped review from Booking.com."""

    username: Optional[str] = Field(
        None,
        description="Display name of the reviewer.",
        json_schema_extra={"example": "John D."},
    )
    user_country: Optional[str] = Field(
        None,
        description="Reviewer's country of origin.",
        json_schema_extra={"example": "United Kingdom"},
    )
    room_view: Optional[str] = Field(
        None,
        description="Room type or room info the reviewer stayed in.",
        json_schema_extra={"example": "Double Room with Sea View"},
    )
    stay_duration: Optional[str] = Field(
        None,
        description="Length of stay (e.g. `3 nights`). Text after the first ` ·` separator is stripped.",
        json_schema_extra={"example": "3 nights"},
    )
    stay_type: Optional[str] = Field(
        None,
        description="Traveller type.",
        json_schema_extra={"example": "Couple"},
    )
    review_post_date: Optional[str] = Field(
        None,
        description="Date the review was posted, format: `MM-DD-YYYY HH:MM:SS`.",
        json_schema_extra={"example": "01-15-2026 14:30:00"},
    )
    review_title: Optional[str] = Field(
        None,
        description="Headline / title of the review.",
        json_schema_extra={"example": "Amazing stay, wonderful staff"},
    )
    rating: Optional[float] = Field(
        None,
        description="Numeric review score on the Booking.com scale (typically 1.0–10.0).",
        json_schema_extra={"example": 8.8},
    )
    original_lang: Optional[str] = Field(
        None,
        description="ISO language code of the original review text (e.g. `en`, `fr`, `de`).",
        json_schema_extra={"example": "en"},
    )
    review_text_liked: Optional[str] = Field(
        None,
        description="Positive part of the review — what the guest liked.",
        json_schema_extra={"example": "Friendly staff and great breakfast."},
    )
    review_text_disliked: Optional[str] = Field(
        None,
        description="Negative part of the review — what the guest disliked.",
        json_schema_extra={"example": "Room was a bit small."},
    )
    full_review: Optional[str] = Field(
        None,
        description="Composite text joining `review_title`, `review_text_liked`, and `review_text_disliked` into one string.",
        json_schema_extra={
            "example": "title: Amazing stay, wonderful staff. liked: Friendly staff and great breakfast. disliked: Room was a bit small."
        },
    )
    en_full_review: Optional[str] = Field(
        None,
        description="Same as `full_review` but only populated when `original_lang` contains `en`. Otherwise `null`.",
    )
    found_helpful: int = Field(
        0,
        description="Number of users who found this review helpful.",
        json_schema_extra={"example": 5},
    )
    found_unhelpful: int = Field(
        0,
        description="Number of users who found this review unhelpful.",
        json_schema_extra={"example": 1},
    )
    owner_resp_text: Optional[str] = Field(
        None,
        description="Hotel owner/management response to the review. `null` if no response.",
        json_schema_extra={"example": "Thank you for your kind words!"},
    )


class ReviewResult(BaseModel):
    """Result object containing the scraped reviews for one task."""

    hotel_name: str = Field(..., description="Hotel name that was scraped.")
    country: str = Field(..., description="Country code of the hotel.")
    type: str = Field("booking_reviews", description="Result type identifier.")
    datetime: str = Field(
        ...,
        description="UTC timestamp when scraping completed, format: `YYYY-MM-DD HH:MM:SS +00:00`.",
    )
    reviews_count: int = Field(
        ..., description="Total number of reviews returned.", ge=0
    )
    items_count: int = Field(
        ..., description="Number of review items in the `items` array.", ge=0
    )
    items: List[ReviewItem] = Field(
        ..., description="Array of individual review objects."
    )


class TaskRecord(BaseModel):
    """A single task entry inside the `tasks` array."""

    id: Optional[str] = Field(
        ...,
        description="Unique task identifier (UUID). Use this to poll results via `task_get`.",
        json_schema_extra={"example": "a1b2c3d4-5678-9abc-def0-1234567890ab"},
    )
    status_code: int = Field(
        ...,
        description=(
            "Task-level status code.\n"
            "- `20100` — Task created / in progress\n"
            "- `20000` — Task completed\n"
            "- `50000` — Task failed"
        ),
        json_schema_extra={"example": 20100},
    )
    status_message: str = Field(
        ...,
        description="Human-readable status description.",
        json_schema_extra={"example": "Task Created."},
    )
    time: str = Field(
        ...,
        description="Execution time for this task.",
        json_schema_extra={"example": "0.0000 sec."},
    )
    cost: float = Field(0, description="Reserved for future billing. Always `0`.")
    result_count: int = Field(
        ...,
        description="Number of result objects. `0` while task is pending.",
        ge=0,
    )
    path: List[str] = Field(
        ...,
        description="API path segments that produced this task.",
        json_schema_extra={"example": ["v3", "reviews", "task_post"]},
    )
    data: Optional[TaskData] = Field(
        None, description="Echo of the original request parameters."
    )
    result: Optional[List[ReviewResult]] = Field(
        None,
        description="Array of result objects. `null` while the task is still in progress.",
    )


class ApiResponse(BaseModel):
    """
    Top-level API response envelope.

    Every endpoint returns this structure. The `tasks` array contains
    one entry per submitted task.
    """

    version: str = Field(
        "3.0.0",
        description="API version.",
    )
    status_code: int = Field(
        ...,
        description=(
            "Overall response status code.\n"
            "- `20000` — Success\n"
            "- `40401` — Task not found\n"
            "- `50000` — Server error"
        ),
        json_schema_extra={"example": 20000},
    )
    status_message: str = Field(
        ...,
        description="Human-readable status for the entire response.",
        json_schema_extra={"example": "Ok."},
    )
    time: str = Field(
        ...,
        description="Server timestamp in UTC, format: `YYYY-MM-DD HH:MM:SS +00:00`.",
    )
    tasks_count: int = Field(
        ..., description="Number of task objects in the `tasks` array.", ge=0
    )
    tasks_error: int = Field(
        ..., description="Number of tasks that encountered errors.", ge=0
    )
    tasks: List[TaskRecord] = Field(
        ..., description="Array of task objects with their statuses and results."
    )


class ReadyItem(BaseModel):
    """A completed task reference returned by `tasks_ready`."""

    id: str = Field(
        ...,
        description="Task ID that is ready for retrieval.",
        json_schema_extra={"example": "a1b2c3d4-5678-9abc-def0-1234567890ab"},
    )
    tag: Optional[str] = Field(
        None,
        description="User-defined tag from the original request, if provided.",
    )
    endpoint: str = Field(
        ...,
        description="Full path to fetch results for this task.",
        json_schema_extra={
            "example": "/v3/reviews/task_get/a1b2c3d4-5678-9abc-def0-1234567890ab"
        },
    )


class ReadyTaskRecord(BaseModel):
    """Task record specifically for the tasks_ready response."""

    id: Optional[str] = Field(None, description="Always `null` for this endpoint.")
    status_code: int = Field(20000)
    status_message: str = Field("Ok.")
    time: str = Field("0.0000 sec.")
    cost: float = Field(0)
    result_count: int = Field(
        ..., description="Number of tasks that are ready.", ge=0
    )
    path: List[str] = Field(
        ..., json_schema_extra={"example": ["v3", "reviews", "tasks_ready"]}
    )
    data: Optional[Any] = Field(None)
    result: List[ReadyItem] = Field(
        ..., description="Array of completed task references."
    )


class TasksReadyResponse(BaseModel):
    """Response for the tasks_ready endpoint."""

    version: str = Field("3.0.0")
    status_code: int = Field(20000)
    status_message: str = Field("Ok.")
    time: str = Field(...)
    tasks_count: int = Field(1)
    tasks_error: int = Field(0)
    tasks: List[ReadyTaskRecord] = Field(...)


# ── Task cleanup ────────────────────────────────────────────────────────────────
def _cleanup_expired_tasks() -> int:
    """Remove completed/failed tasks older than TASK_TTL_SECONDS.

    Returns:
        Number of tasks removed.
    """
    now = time_module.time()
    expired = []
    with _tasks_store_lock:
        for tid, task in tasks_store.items():
            finished_at = task.get("_finished_at")
            if finished_at and (now - finished_at) > TASK_TTL_SECONDS:
                expired.append(tid)
        for tid in expired:
            del tasks_store[tid]
    return len(expired)


# ── Background scraping worker ──────────────────────────────────────────────────
def _run_scrape(task_id: str, params: dict) -> None:
    """Execute the scraping job in a background thread."""
    # Clean up expired tasks before starting new work
    _cleanup_expired_tasks()

    start = time_module.perf_counter()
    try:
        input_params = {
            "hotel_name": params["hotel_name"],
            "country": params["country"].lower(),
            "sort_by": params.get("sort_by", "most_relevant"),
            "n_rows": params.get("depth", -1),
        }

        s = Scrape(input_params, save_data_to_disk=False)
        reviews = s.run()
        elapsed = f"{time_module.perf_counter() - start:.4f} sec."

        with _tasks_store_lock:
            tasks_store[task_id].update(
                {
                    "status_code": 20000,
                    "status_message": "Ok.",
                    "time": elapsed,
                    "result_count": 1,
                    "_finished_at": time_module.time(),
                    "result": [
                        {
                            "hotel_name": params["hotel_name"],
                            "country": params["country"].lower(),
                            "type": "booking_reviews",
                            "datetime": datetime.now(timezone.utc).strftime(
                                "%Y-%m-%d %H:%M:%S +00:00"
                            ),
                            "reviews_count": len(reviews),
                            "items_count": len(reviews),
                            "items": reviews,
                        }
                    ],
                }
            )
    except Exception as e:
        elapsed = f"{time_module.perf_counter() - start:.4f} sec."
        with _tasks_store_lock:
            tasks_store[task_id].update(
                {
                    "status_code": 50000,
                    "status_message": f"Error: {e}",
                    "time": elapsed,
                    "result_count": 0,
                    "_finished_at": time_module.time(),
                    "result": None,
                }
            )


# ── Helpers ──────────────────────────────────────────────────────────────────────
def _build_response(
    tasks: list,
    tasks_error: int = 0,
    status_code: int = 20000,
    status_message: str = "Ok.",
) -> dict:
    return {
        "version": "3.0.0",
        "status_code": status_code,
        "status_message": status_message,
        "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S +00:00"),
        "tasks_count": len(tasks),
        "tasks_error": tasks_error,
        "tasks": tasks,
    }


# ── Endpoints ────────────────────────────────────────────────────────────────────
@app.get(
    "/health",
    summary="Health check",
    description="Returns `ok` if the server is running.",
    tags=["System"],
)
def health():
    return {"status": "ok"}


@app.post(
    "/v3/reviews/task_post",
    response_model=ApiResponse,
    summary="Create scraping task(s)",
    description=(
        "Submit one or more hotel review scraping tasks. Each task is queued and "
        "processed asynchronously in the background.\n\n"
        "The response returns immediately with a unique `id` per task "
        "(status `20100 — Task Created`). Use this ID to poll for results "
        "via `GET /v3/reviews/task_get/{id}`.\n\n"
        "You can submit multiple tasks in a single request by passing "
        "an array with multiple objects."
    ),
    response_description="Envelope with one task record per submitted item, each containing a unique task `id`.",
    tags=["Reviews"],
)
def task_post(items: List[TaskPostItem]):
    created_tasks = []

    for item in items:
        task_id = str(uuid.uuid4())

        task_record = {
            "id": task_id,
            "status_code": 20100,
            "status_message": "Task Created.",
            "time": "0.0000 sec.",
            "cost": 0,
            "result_count": 0,
            "path": ["v3", "reviews", "task_post"],
            "data": {
                "api": "reviews",
                "function": "task_post",
                "hotel_name": item.hotel_name,
                "country": item.country.lower(),
                "sort_by": item.sort_by,
                "depth": item.depth,
                "tag": item.tag,
            },
            "result": None,
        }

        with _tasks_store_lock:
            tasks_store[task_id] = task_record.copy()

        _scrape_pool.submit(_run_scrape, task_id, item.model_dump())

        created_tasks.append(task_record)

    return _build_response(created_tasks)


@app.get(
    "/v3/reviews/tasks_ready",
    response_model=TasksReadyResponse,
    summary="List completed tasks",
    description=(
        "Returns a list of all tasks that have finished processing "
        "(status `20000`) and whose results are ready to be retrieved.\n\n"
        "Each item includes the task `id`, optional `tag`, and the full "
        "`endpoint` path to fetch results."
    ),
    response_description="Envelope with a single task record whose `result` array lists all ready task references.",
    tags=["Reviews"],
)
def tasks_ready():
    ready = []
    with _tasks_store_lock:
        for tid, task in tasks_store.items():
            if task.get("status_code") == 20000:
                ready.append(
                    {
                        "id": tid,
                        "tag": task.get("data", {}).get("tag"),
                        "endpoint": f"/v3/reviews/task_get/{tid}",
                    }
                )

    return _build_response(
        [
            {
                "id": None,
                "status_code": 20000,
                "status_message": "Ok.",
                "time": "0.0000 sec.",
                "cost": 0,
                "result_count": len(ready),
                "path": ["v3", "reviews", "tasks_ready"],
                "data": None,
                "result": ready,
            }
        ]
    )


@app.get(
    "/v3/reviews/task_get/{task_id}",
    response_model=ApiResponse,
    summary="Get task results",
    description=(
        "Retrieve the scraping results for a specific task by its UUID.\n\n"
        "**Possible `status_code` values in the task record:**\n"
        "- `20100` — Task is still being processed. `result` will be `null`.\n"
        "- `20000` — Task completed. `result` contains the review data.\n"
        "- `50000` — Task failed. Check `status_message` for the error.\n\n"
        "If the task ID does not exist, the top-level `status_code` will be `40401`."
    ),
    response_description="Envelope with a single task record containing the scraping results (or null if pending).",
    tags=["Reviews"],
    responses={
        200: {
            "description": "Task found (may be pending, completed, or failed)",
        },
        404: {
            "description": "Task ID not found",
            "content": {
                "application/json": {
                    "example": {
                        "version": "3.0.0",
                        "status_code": 40401,
                        "status_message": "Task Not Found.",
                        "time": "2026-01-15 12:00:00 +00:00",
                        "tasks_count": 0,
                        "tasks_error": 1,
                        "tasks": [],
                    }
                }
            },
        },
    },
)
def task_get(
    task_id: str,
    limit: int = Query(
        default=0,
        ge=0,
        description=(
            "Maximum number of review items to return. "
            "Use `0` (default) to return all items."
        ),
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of review items to skip before returning results.",
    ),
):
    task = tasks_store.get(task_id)
    if not task:
        return _build_response(
            tasks=[],
            tasks_error=1,
            status_code=40401,
            status_message="Task Not Found.",
        )

    result = task.get("result")

    # Apply pagination to review items if the task has results
    if result and limit > 0:
        paginated_result = []
        for r in result:
            r_copy = dict(r)
            items = r_copy.get("items", [])
            total = len(items)
            sliced = items[offset : offset + limit]
            r_copy["items"] = sliced
            r_copy["items_count"] = len(sliced)
            r_copy["total_count"] = total
            paginated_result.append(r_copy)
        result = paginated_result
    elif result and offset > 0:
        paginated_result = []
        for r in result:
            r_copy = dict(r)
            items = r_copy.get("items", [])
            total = len(items)
            sliced = items[offset:]
            r_copy["items"] = sliced
            r_copy["items_count"] = len(sliced)
            r_copy["total_count"] = total
            paginated_result.append(r_copy)
        result = paginated_result

    return _build_response(
        [
            {
                "id": task["id"],
                "status_code": task["status_code"],
                "status_message": task["status_message"],
                "time": task["time"],
                "cost": 0,
                "result_count": task.get("result_count", 0),
                "path": ["v3", "reviews", "task_get", task_id],
                "data": task["data"],
                "result": result,
            }
        ]
    )
