"""Load last.fm data into sqlite."""
import collections
import dataclasses
import datetime
import json
import logging
import pathlib
import sqlite3

import pandas as pd

logging.basicConfig(level=logging.INFO)

# Config
SQL_PATH = pathlib.Path("../data/dataset.sqlite")
DATA_PATH = pathlib.Path("../data/lastfm")
TRACKS_PATH = DATA_PATH / "tracks"
ARTISTS_PATH = DATA_PATH / "artists"


# Data model (Python)
@dataclasses.dataclass
class LastFMMapping:
    lastfm_id: int
    artist_name: str
    song_name: str


@dataclasses.dataclass
class LastFMTag:
    lastfm_id: int
    tag: str
    url: str


@dataclasses.dataclass
class LastFMData:
    lastfm_id: int
    listeners: int
    playcount: int
    duration: int
    mbid: str = None
    artist_mbid: str = None


@dataclasses.dataclass
class LastFMArtist:
    artist_mbid: str
    listeners: int
    playcount: int


def main() -> None:
    _start_time = datetime.datetime.now()
    # Define three tables: listens / tags / product2chart
    conn = sqlite3.connect(SQL_PATH)
    tables = collections.defaultdict(list)

    # First, process the tracks.
    for idx, file in enumerate(
            TRACKS_PATH.glob("*.json")
    ):
        logging.info(f"Processing {file}...")
        with open(file, "r") as f:
            data = json.load(f)
        track_data = data["track"]
        # Create the mapping table so we can link it back to the charts.
        product_ids = file.stem.split(",")
        tables["link_lastfm_chart"].extend(
            [
                LastFMMapping(lastfm_id=idx, artist_name=data["artist_name"], song_name=data["song_name"])
                for product_id in product_ids
            ]
        )

        # Load the tags into a separate table
        tags = track_data.get("toptags", {}).get("tag", [])
        tables["lastfm_tags"].extend(
            [LastFMTag(lastfm_id=idx, url=tag["url"], tag=tag["name"]) for tag in tags]
        )

        # Finally, prep the core data
        lastfm = LastFMData(
            lastfm_id=idx,
            mbid=track_data.get("mbid"),
            listeners=int(track_data.get("listeners", 0)) or None,
            playcount=int(track_data.get("playcount", 0)) or None,
            duration=int(track_data.get("duration", 0)) or None,
            artist_mbid=track_data.get("artist", {}).get("mbid", None),
        )
        tables["lastfm"].append(lastfm)

    # Then process the artists.
    for file in ARTISTS_PATH.glob("*.json"):
        logging.info(f"Processing {file}...")
        with open(file, "r") as f:
            artist_data = json.load(f)
            # Issue with the serialisation.
            if isinstance(artist_data, str):
                artist_data = json.loads(artist_data)
        tables["lastfm_artists"].append(LastFMArtist(artist_mbid=artist_data["artist"]["mbid"],
                                                     listeners=int(artist_data["artist"]["stats"]["listeners"]),
                                                     playcount=int(artist_data["artist"]["stats"]["playcount"])))
    for table_name, data in tables.items():
        logging.info(f"Writing table {table_name} to sqlite...")
        df = pd.DataFrame([dataclasses.asdict(datum) for datum in data])
        print(df)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    _end_time = datetime.datetime.now()
    logging.info(f"Finished in {(_end_time - _start_time).seconds}")


if __name__ == "__main__":
    main()
