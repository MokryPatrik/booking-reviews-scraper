from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from core.scrape import Scrape

app = FastAPI(
    title="Booking.com Reviews Parser",
    description="API to scrape hotel reviews from Booking.com and return them as JSON",
    version="1.0.0",
)


class ReviewResponse(BaseModel):
    hotel_name: str
    country: str
    sort_by: str
    total_reviews: int
    reviews: List[dict]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reviews", response_model=ReviewResponse)
def get_reviews(
    hotel_name: str = Query(
        ..., description="Hotel name from booking.com URL slug", min_length=2
    ),
    country: str = Query(
        ...,
        description="Two character country code (ALPHA-2), e.g. 'us', 'es', 'gb'",
        min_length=2,
        max_length=2,
    ),
    sort_by: str = Query(
        "most_relevant",
        description="Sort reviews by: most_relevant, newest_first, oldest_first, highest_scores, lowest_scores",
    ),
    n_reviews: int = Query(
        -1,
        description="Number of reviews to scrape. -1 means all reviews",
    ),
):
    """Scrape hotel reviews from Booking.com and return as JSON."""
    try:
        input_params = {
            "hotel_name": hotel_name,
            "country": country.lower(),
            "sort_by": sort_by,
            "n_rows": n_reviews,
        }

        s = Scrape(input_params, save_data_to_disk=False)
        reviews = s.run()

        return ReviewResponse(
            hotel_name=hotel_name,
            country=country.lower(),
            sort_by=sort_by,
            total_reviews=len(reviews),
            reviews=reviews,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
