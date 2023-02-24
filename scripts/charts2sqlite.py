"""Dump the chart data into a sqlite database."""
import dataclasses
import datetime
import glob
import logging
import pathlib
import sqlite3
from typing import List

import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)


@dataclasses.dataclass
class ChartData:
    week_ending: datetime.date
    chart_id: str
    product_id: str
    artist_name: str
    song_name: str
    peak_position: int
    weeks_on_chart: int
    position: int
    label_name: str


def _extract_chart_data(data: str) -> List[ChartData]:
    results = []
    soup = BeautifulSoup(data, "html.parser")
    chart_table = soup.find("table", {"class": "chart-positions"})

    for row in chart_table.find_all("tr", {"class": None}):

        # Skip the row if it doesn't contain any chart data.
        if row.find("span", {"class": "position"}) is None:
            continue

        # Extract product catalogue data as well.
        product_data = row.find("a", {"class": "chart-runs-icon"})
        chart_id = product_data.attrs["data-chartid"]
        product_id = product_data.attrs["data-productid"]
        week = datetime.datetime.strptime(chart_id.split("-")[-1], "%Y%m%d")

        # Marshall into ChartData class prior to writing to sqlite.
        chart_data = ChartData(
            chart_id=chart_id,
            product_id=product_id,
            peak_position=int(row.find_all("td")[-4].text),
            weeks_on_chart=int(row.find_all("td")[-3].text),
            week_ending=week,
            position=int(row.find("span", {"class": "position"}).text),
            song_name=row.find("div", {"class": "title"}).text.strip(),
            artist_name=row.find("div", {"class": "artist"}).text.strip(),
            label_name=row.find("span", {"class": "label"}).text.strip(),
        )
        results.append(chart_data)
    return results


def main():
    _start_time = datetime.datetime.now()
    DATA_PATH = pathlib.Path("../data/charts/singles-chart")
    SQL_PATH = pathlib.Path("../data/dataset.sqlite")
    chart_data = []
    for file in list(DATA_PATH.glob("*.html")):
        logging.info(f"Processing file {file}...")
        with open(file, "r") as f:
            data = f.read()
        chart_data.extend(_extract_chart_data(data))
    df = pd.DataFrame([dataclasses.asdict(data) for data in chart_data])
    conn = sqlite3.connect(SQL_PATH)
    df.to_sql("charts", conn)
    _end_time = datetime.datetime.now()
    logging.info(f"Finished processing in {(_end_time - _start_time).seconds}...")


if __name__ == "__main__":
    main()
