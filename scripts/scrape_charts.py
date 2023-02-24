"""Dump the officialcharts.com website content."""
import asyncio
import datetime
import logging
import math
import pathlib

import httpx

logging.basicConfig(level=logging.INFO)

# Configuration.
DATA_DIR = pathlib.Path("../data/charts")
INTERVAL = datetime.timedelta(days=7)  # Charts are weekly.
START_DATE = datetime.date(1979, 12, 30)
END_DATE = datetime.date(2010, 1, 1)
URL_TEMPLATE = "https://www.officialcharts.com/charts/{chart_type}/%Y%m%d/{chart_id}/"
CHART_TYPES = {"singles-chart": 7501}

SEMAPHORE = asyncio.Semaphore(16)
"""Limit the number of async tasks to 16 to prevent timeouts or other issues."""

STEPS = math.ceil((END_DATE - START_DATE).days / INTERVAL.days)
"""Compute the number of steps we need to take to load all of the pages."""


async def _download_chart_page(
    session, chart_date: datetime.date, chart_type: str, chart_id: int
) -> httpx.Response:
    """Scrape a chart page."""
    logging.info(
        chart_date.strftime(f"Processing chart {chart_type}:{chart_id}:%Y-%m-%d...")
    )
    template = URL_TEMPLATE.format(chart_type=chart_type, chart_id=chart_id)
    url = chart_date.strftime(template)
    response = await session.get(url)
    return response


async def _save_chart_data(
    data: httpx.Response, chart_date: datetime.date, chart_type: str
) -> None:
    """Write the chart data to the data directory."""
    output_path = DATA_DIR / chart_type
    output_file = chart_date.strftime("%Y%m%d.html")

    # Create the path if it doesn't exist.
    output_path.mkdir(parents=True, exist_ok=True)

    # Save the content to disk.
    with open(output_path / output_file, "wb") as f:
        f.write(data.content)


async def _download_and_save_chart_data(
    session: httpx.AsyncClient,
    chart_date: datetime.date,
    chart_type: str,
    chart_id: int,
):
    """Orchestrate the download and saving functions."""
    async with SEMAPHORE:
        data = await _download_chart_page(
            session, chart_date=chart_date, chart_type=chart_type, chart_id=chart_id
        )
        await _save_chart_data(data, chart_date=chart_date, chart_type=chart_type)


async def main():
    """Scrape and dump the UK chart data."""
    _start_time = datetime.datetime.now()
    for chart_type, chart_id in CHART_TYPES.items():
        logging.info(f"Processing {chart_type}:{chart_id}...")

        # Async scrape the chart data up to `SEMAPHORE` weeks at a time.
        async with httpx.AsyncClient() as session:
            await asyncio.gather(
                *[
                    _download_and_save_chart_data(
                        session,
                        chart_date=chart_date,
                        chart_type=chart_type,
                        chart_id=chart_id,
                    )
                    for chart_date in (START_DATE + n * INTERVAL for n in range(STEPS))
                ]
            )
    _end_time = datetime.datetime.now()
    logging.info(f"Finished processing in {(_end_time - _start_time).seconds}...")


if __name__ == "__main__":
    # Required to avoid any asyncio errors with what event loop we're running in.
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
