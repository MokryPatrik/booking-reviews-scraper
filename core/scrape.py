import concurrent.futures
import csv
import logging
import os
import re
import string
import sys
import threading
import time
from datetime import datetime
from typing import List, Union
from urllib.parse import parse_qs, urlparse

import requests
import urllib3
import yaml
from bs4 import BeautifulSoup
from dateutil import parser

from core.data_models import Config, Input, sort_by_map

# Timeout in seconds for HTTP requests to prevent hung threads
REQUEST_TIMEOUT = 30

# Suppress SSL warnings when using Web Unblocker proxy (requires verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _create_session():
    """Create a requests session with optional Web Unblocker proxy support.

    Set the PROXY_URL env var to route requests through a proxy:
        export PROXY_URL="http://user:pass@unblocker.iproyal.com:12323"
    """
    s = requests.Session()
    s.verify = False  # Web Unblocker uses its own SSL handling

    proxy_url = os.environ.get("PROXY_URL")
    if proxy_url:
        s.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

    return s


session = _create_session()


class Scrape:
    def __init__(self, input: dict, save_data_to_disk=True, logger=None) -> None:
        if "job_id" not in os.environ:
            os.environ["job_id"] = str(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))

        if logger is not None:
            self.logger = logger
        else:
            self._get_logger()
            self.logger = logging.getLogger(f"booking_parser.{os.getenv('job_id', 'default')}")

        self._config = self._load_config()
        self.input_params = Input(**input)

        proxy_url = os.environ.get("PROXY_URL")
        if proxy_url:
            # Mask credentials in log output
            self.logger.info(f"Using proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
        else:
            self.logger.warning("No PROXY_URL set — requests will use direct connection")

        # the below property is for the purpose of monitoring progress
        # It contains the parsed reviews of processed pages and their idx
        self._parsed_pages_reviews = []  # list of reviews for each page
        self._lock = threading.Lock()  # protects _parsed_pages_reviews
        self._execution_finished = (
            threading.Event()
        )  # set this event when execution is finished

        st_ = ""
        for key, value in self.input_params.model_dump().items():
            st_ += f"-->  {key}: {value}\n"
        self.logger.info(
            f"\n\n******** Input Params ********\n{st_}************************\n\n"
        )

        self._LOCAL_OUTPUT_PATH = "{output_dir}/{entity_name}_" + str(
            os.getenv("job_id")
        )
        self._save_data_to_disk = save_data_to_disk

    def _get_logger(self):
        if not os.path.isdir("logs"):
            os.mkdir("logs")

        # Create a file handler
        file_path = f"logs/{os.getenv('job_id')}.log"
        frmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        # Use a named logger to avoid polluting the root logger
        logger = logging.getLogger(f"booking_parser.{os.getenv('job_id', 'default')}")
        logger.setLevel(logging.INFO)

        # Only add handlers if this logger doesn't already have them
        if not logger.handlers:
            # Log to console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(frmt)
            console_handler.setFormatter(console_formatter)

            # Log to file
            file_handler = logging.FileHandler(file_path)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(frmt)
            file_handler.setFormatter(file_formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

    def _progress_thread_start(self, ls_urls: List[dict]):
        """It will keep printing the overall progress

        Args:
            ls_urls: list of total urls found
        """
        self.logger.info("Progress Monitoring Thread Started")
        prev = 0
        while not self._execution_finished.is_set():
            time.sleep(2)
            if len(self._parsed_pages_reviews):
                ln = len(self._parsed_pages_reviews)
                if ln > prev:
                    self.logger.info(f"Processed {ln}/{len(ls_urls)}")
                    prev = ln

    def _save_local_files(
        self,
        ls_reviews: List[dict] = None,
    ):
        """save local csv files. It creates a direcotry based on "entity_name" and stores
        two csv files in it.

        - reviews.csv: contains reviews

        Args:
            ls_reviews: list of review objects

        """
        dir_path = self._LOCAL_OUTPUT_PATH.format(
            output_dir=self._config.OUTPUT_DIR, entity_name=self.input_params.hotel_name
        )

        if not os.path.exists(dir_path) and (ls_reviews):
            os.makedirs(dir_path)

        fname = f"{dir_path}/reviews_{self.input_params.sort_by}.csv"

        if ls_reviews:
            write_header = True
            if os.path.exists(fname):
                write_header = False

            with open(fname, "a", newline="") as file:
                try:
                    writer = csv.writer(file)
                    if write_header:
                        writer.writerow(ls_reviews[0].keys())
                    for row in ls_reviews:
                        writer.writerow(row.values())

                except Exception as ex:
                    self.logger.error(ex)

    def _load_config(self) -> Config:
        """Loads config.yml"""
        config = None
        with open("config.yml", "r") as file:
            config: dict = yaml.safe_load(file)

        config = Config(**config)
        return config

    ##########################################################
    # ******** Scraping Logic Methods ********
    ##########################################################

    def _get_max_offset_parameter(self) -> int:
        """Returns the maximum value of offset parameter based on the total number of pages in the html.
        Offset parameter controls the page number. Page 1 has offset = 0 or no value. Page 2 will have offset=10
        then Page 3 will have offset=20 and so on.

        Returns:
            value of offset parameter. 0 when there is only one review page
        """
        self.logger.info("Checking max offset parameter value")

        r = session.get(
            self._config.HOTEL_REVIEWS_PAGE,
            params={
                "cc1": self.input_params.country,
                "pagename": self.input_params.hotel_name,
                "rows": 10,
            },
            timeout=REQUEST_TIMEOUT,
        )

        soup = BeautifulSoup(r.content.decode(), "html.parser")
        a_elements_with_span = [
            a
            for a in soup.select(
                "div.bui-pagination__pages > div.bui-pagination__list > div.bui-pagination__item > a"
            )
            if a.find("span") and "Page " in a.text
        ]

        # If there are more than one pages. It means we should have the offset parameter
        if a_elements_with_span:
            if a_elements_with_span[-1].has_attr("href"):
                # get the href from the a element

                # Parse the URL
                parsed_url = urlparse(a_elements_with_span[-1]["href"])

                # Extract the query parameters as a dictionary
                query_parameters = parse_qs(parsed_url.query)

                offset: str = query_parameters["offset"]
                if isinstance(offset, list):
                    offset = offset[0]
                if ";" in offset:
                    offset = offset.split(";")[0]

                if not offset.isdigit():
                    self.logger.error(f"Offset paramter is non-digit: {offset}")

                self.logger.info(f"Offset parameter max value: {offset}")

                return int(offset)
            else:
                self.logger.error(
                    f"Page number link <a> does not have href attribute: {a_elements_with_span[-1]}"
                )

        # If there is only one reviews page then there is no offset parameter
        else:
            self.logger.info("No offset parameter found")

        return 0

    def _create_urls(self):
        """It creates list of urls of review pages based on the total reivews
        number of reviews.

        Args:
            hotel_name: name of the hotel on booking.com
        """
        ls_urls = []
        self.logger.info("Creating URLs")
        param_offset_max: int = self._get_max_offset_parameter()

        # ********** BASED ON TOTAL REVIEW PAGES: CREATE LIST OF URLS TO SCRAPE **********

        sort_by = sort_by_map[self.input_params.sort_by]

        offset_counter = 0
        params = {
            "cc1": self.input_params.country,
            "pagename": self.input_params.hotel_name,
            "rows": 10,
            "sort": sort_by,
        }

        while offset_counter <= param_offset_max:
            # when offset_counter=0 we really don't need its value
            if offset_counter:
                params["offset"] = offset_counter

            url = (
                requests.Request("GET", self._config.HOTEL_REVIEWS_PAGE, params=params)
                .prepare()
                .url
            )
            ls_urls.append({"idx": offset_counter, "url": url})
            offset_counter += 10

        self.logger.info(f"Created URLs: {len(ls_urls)}")
        return ls_urls

    def _scrape(self, url_dict: dict) -> dict:
        """Returns the response of the the passed url

        Args:
            url_dict: dict containing the urls and idx/offset_param of the current url/page

        Returns:
            response object
        """
        response = None
        url = url_dict["url"]  # url of the reviews page
        idx = url_dict["idx"]  # orginal offset_param value / id of reviews page

        retry_count = 1
        while retry_count <= self._config.MAX_RETIES:
            response = session.get(url, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                break

            else:
                self.logger.warning(
                    f"Retrying {retry_count} (status={response.status_code}) ... {url}"
                )
                time.sleep(retry_count * 2)  # exponential backoff between retries
                retry_count += 1
                continue

        return {"idx": idx, "response": response}

    def _validate(self, element):
        """
        Removes multitples spaces and strips \n

        Args:
            element: Beautiful Soap element

        Returns:
            string text extracted from element
        """
        if element is not None:
            if isinstance(element, str):
                text = re.sub(r"\s+", " ", element).strip(" \n")
            else:
                text = re.sub(r"\s+", " ", element.text).strip(" \n")
            if len(text):
                return text

        return None

    def _parse_scraped_results(
        self, ls_response: List[dict]
    ) -> Union[List[dict], None]:
        """Takes response objects containing html content of a single reviews pages, and parses the data

        Args:
            ls_response: list of dicts with page-idx and response objects with html { idx and requests.Response object}
            shared_ls_results: process safe list for appending resutlts

        Returns:
            None when shared list is passed, otherwise [ {idx of the review page, list of reviews in that page}, ... ]
        """

        pages_reviews = []

        for (
            response_dict
        ) in ls_response:  # iterate on the response objects of review pages
            page_reviews = []
            idx = response_dict["idx"]
            response: requests.Response = response_dict[
                "response"
            ]  # current response object

            soup = BeautifulSoup(response.content.decode(), "html.parser")
            reviews = soup.select("ul.review_list > li")

            for i in range(
                len(reviews)
            ):  # iterate on the review items of the current page
                review = reviews[i]
                username = self._validate(
                    review.select_one(
                        "div.c-review-block__guest span.bui-avatar-block__title"
                    )
                )  # .text.strip(' \n')
                user_country = self._validate(
                    review.select_one(
                        "div.c-review-block__guest span.bui-avatar-block__subtitle"
                    )
                )  # .text.strip(' \n')
                room_view = self._validate(
                    review.select_one(
                        "div.c-review-block__room-info-row div.bui-list__body"
                    )
                )  # .text.strip(' \n')

                stay_duration = self._validate(
                    review.select_one("ul.c-review-block__stay-date div.bui-list__body")
                )
                stay_duration = (
                    stay_duration.split(" ·")[0] if stay_duration is not None else None
                )

                stay_type = self._validate(
                    review.select_one(
                        "ul.review-panel-wide__traveller_type div.bui-list__body"
                    )
                )  # .text.strip(' \n')
                review_title = self._validate(
                    review.select_one("h3.c-review-block__title")
                )  # .text.strip(' \n')

                # Use a lambda function to find the element with inner text containing "Received"
                date = self._validate(
                    review.find(
                        lambda tag: tag.name == "span" and "Reviewed:" in tag.get_text()
                    )
                )

                if date:
                    date = date.split(":")[-1].strip()
                    date = parser.parse(date).strftime("%m-%d-%Y %H:%M:%S")

                rating = self._validate(
                    review.select_one("div.bui-review-score__badge")
                )  # .text.strip(' \n')
                rating = float(rating) if rating is not None else rating
                review_text = review.select("div.c-review span.c-review__body")

                review_text_liked = None
                review_text_disliked = None
                original_lang = None
                full_review, en_full_review = None, None
                if review_text:
                    review_text_liked = self._validate(review_text[0])
                    if (
                        "There are no comments available for this review".lower()
                        in review_text_liked.lower()
                    ):
                        review_text_liked = None
                    original_lang = review_text[0].get("lang", default=None)

                    if len(review_text) > 1:
                        review_text_disliked = self._validate(review_text[1])
                        if review_text_disliked is None:
                            if len(review_text) > 2:
                                review_text_disliked = self._validate(review_text[2])

                # Add '.' period sign to the end of each part of the review. If its not already there
                t_title = f"{review_title}" if review_title else ""
                t_title = (
                    f"{t_title}."
                    if t_title and t_title[-1] not in string.punctuation
                    else t_title
                )

                t_liked = f"liked: {review_text_liked}" if review_text_liked else ""
                t_liked = (
                    f"{t_liked}."
                    if t_liked and t_liked[-1] not in string.punctuation
                    else t_liked
                )

                t_disliked = (
                    f"disliked: {review_text_disliked}" if review_text_disliked else ""
                )
                t_disliked = (
                    f"{t_disliked}."
                    if t_disliked and t_disliked[-1] not in string.punctuation
                    else t_disliked
                )

                full_review = f"{t_title} {t_liked} {t_disliked}"
                full_review = self._validate(full_review)
                # ------------------------------------------------

                if original_lang and "en" in original_lang:
                    en_full_review = full_review

                found_helpful = self._validate(
                    review.select_one(
                        "div.c-review-block__row--helpful-vote p.review-helpful__vote-others-helpful"
                    )
                )

                found_helpful = (
                    0
                    if found_helpful is None
                    else int(
                        found_helpful.split("people")[0].strip()
                        if "people" in found_helpful
                        else found_helpful.split("person")[0].strip()
                    )
                )
                found_unhelpful = self._validate(
                    review.select_one(
                        "div.c-review-block__row--helpful-vote p.--unhelpful"
                    )
                )
                found_unhelpful = (
                    0
                    if found_unhelpful is None
                    else int(
                        found_unhelpful.split("people")[0].strip()
                        if "people" in found_unhelpful
                        else found_unhelpful.split("person")[0].strip()
                    )
                )

                owner_response = review.select(
                    "div.c-review-block__response span.c-review-block__response__body"
                )
                if owner_response:
                    owner_response = self._validate(owner_response[-1])
                else:
                    owner_response = None

                res = {
                    "username": username,
                    "user_country": user_country,
                    "room_view": room_view,
                    "stay_duration": stay_duration,
                    "stay_type": stay_type,
                    "review_post_date": date,
                    "review_title": review_title,
                    "rating": rating,
                    "original_lang": original_lang,
                    "review_text_liked": review_text_liked,
                    "review_text_disliked": review_text_disliked,
                    "full_review": full_review,
                    "en_full_review": en_full_review,
                    "found_helpful": found_helpful,
                    "found_unhelpful": found_unhelpful,
                    "owner_resp_text": owner_response,
                }
                page_reviews.append(res)

            # idx: orginal offset_param value / id of reviews page
            # reviews: list of reviews found on the page
            with self._lock:
                self._parsed_pages_reviews.append({"idx": idx, "reviews": page_reviews})
            pages_reviews.append({"idx": idx, "reviews": page_reviews})

        return pages_reviews

    ##########################################################
    # ******** Scraping Modes full/partial ********
    ##########################################################

    def _scrape_and_parse(self, url_dict: dict) -> dict:
        """Fetch a single review page and parse it immediately.

        This avoids holding raw HTML responses in memory — the response
        is discarded as soon as the page is parsed.

        Args:
            url_dict: dict containing the url and idx/offset_param

        Returns:
            dict with idx and parsed reviews list
        """
        res_dict = self._scrape(url_dict)
        parsed = self._parse_scraped_results([res_dict])
        return parsed[0] if parsed else {"idx": url_dict["idx"], "reviews": []}

    def _get_all_reviews(self, ls_urls: List[dict]) -> List[dict]:
        """Gets all the reviews till the last page.

        Uses a ThreadPoolExecutor to fetch and parse each page incrementally.
        Each page's HTML response is discarded immediately after parsing,
        keeping peak memory usage proportional to the number of concurrent
        workers rather than the total number of pages.

        Args:
            ls_urls: list containing url and idx/offset_param of each reviews page

        Returns:
            list of all the reviews
        """
        _start = time.time()
        self.logger.info(f"Starting scrape & parse on {len(ls_urls)} urls")

        parsed_pages = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config.REQUESTS_PER_SECOND
        ) as executor:
            # Submit tasks in rate-limited batches
            futures = []
            cnt = 0
            for url_dict in ls_urls:
                f = executor.submit(self._scrape_and_parse, url_dict)
                futures.append(f)
                cnt += 1

                if cnt >= self._config.REQUESTS_PER_SECOND:
                    time.sleep(1)
                    cnt = 0

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                parsed_pages.append(future.result())

        # Sort the list based on the 'idx' key so that the reviews
        # of the first page come first
        parsed_pages.sort(key=lambda x: x["idx"])
        result_list = []
        for page in parsed_pages:
            result_list.extend(page["reviews"])

        self.logger.info(
            f"Finished scrape & parse: {len(result_list)} reviews in {time.time() - _start:.1f} seconds"
        )

        return result_list

    def _get_cond_reviews(self, ls_urls: List[dict]) -> List[dict]:
        """Gets reviews based on any filter either n_rows or stoping criteria

        Args:
        ls_urls: list containing url and idx/offset_param of each reviews page

        Returns:
        list of selected the reviews

        """

        _start = time.time()
        self.logger.info(f"Starting Conditional Scraping on {len(ls_urls)} urls")

        count_review = 0
        ls_reviews = []
        stop_criteria_met = False

        for url_dict in ls_urls:
            res_dict: dict = self._scrape(
                url_dict
            )  # will return --> {"idx": idx, "response": response}
            ls_res = self._parse_scraped_results(
                [res_dict]
            )  # will return --> [{"idx": idx, "reviews": []}]
            if ls_res:
                reviews = ls_res[0]["reviews"]
                count_review += len(reviews)

                if self.input_params.stop_critera:
                    for review_obj in reviews:
                        if (
                            self.input_params.stop_critera.username.lower().strip()
                            == review_obj["username"].lower().strip()
                        ):
                            r_title = (
                                ""
                                if review_obj["review_title"] is None
                                else review_obj["review_title"].lower().strip()
                            )

                            if (
                                self.input_params.stop_critera.review_text_title.lower().strip()
                                in r_title
                            ):
                                stop_criteria_met = True
                                break

                        ls_reviews.append(review_obj)

                else:
                    ls_reviews.extend(reviews)

                if stop_criteria_met:
                    break

                if -1 < self.input_params.n_rows <= count_review:
                    ls_reviews = ls_reviews[: self.input_params.n_rows]
                    break

        self.logger.info(
            f"Finished Conditional Scraping: {len(ls_reviews)} in {time.time() - _start:.1f} seconds"
        )
        return ls_reviews

    ##########################################################
    # ******** Main Executable Method ********
    ##########################################################

    def run(self) -> List[dict]:
        """
        Main function which executes the module
        """

        _start = time.time()
        results = []
        ls_urls = self._create_urls()
        prog_thd = threading.Thread(target=self._progress_thread_start, args=(ls_urls,))
        prog_thd.start()

        if self.input_params.n_rows == -1 and self.input_params.stop_critera is None:
            # it means to get all the reviews, based on the provided/default sort_by option
            results = self._get_all_reviews(ls_urls)

        else:
            results = self._get_cond_reviews(ls_urls)

        self.logger.info(f"Process complete {time.time() - _start:.1f} seconds")
        self.logger.info(f"Reviews found: {len(results)}")

        self._execution_finished.set()  # to stop the monitoring thread

        if self._save_data_to_disk:
            self._save_local_files(results)

        return results
