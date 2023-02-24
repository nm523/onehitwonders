"""Dump data from last.fm to disk."""
import asyncio
import dataclasses
import datetime
import json
import logging
import pathlib
import sqlite3
from typing import Any, Dict, List

import httpx
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)

API_KEY = ""
SHARED_SECRET = ""
ENDPOINT = "http://ws.audioscrobbler.com/2.0/"
DATA_PATH = pathlib.Path("../data")

ONE_HIT_WONDERS_QUERY = """SELECT * FROM v_one_hit_wonders"""



@dataclasses.dataclass
class Song:
    artist: str
    name: str


def _get_unique_songs(conn: sqlite3.Connection) -> List[Song]:
    # Map songs to their product ids (can have more than one).
    df = pd.read_sql_query(ONE_HIT_WONDERS_QUERY, conn)
    return [
        Song(artist=row.artist_name, name=row.song_name)
        for _, row in df.iterrows()
    ]


def _request_track_info(session: requests.Session, song: Song) -> Dict[str, Any]:
    params = {
        "method": "track.getInfo",
        "api_key": API_KEY,
        "artist": song.artist,
        "track": song.name,
        "format": "json",
        "autocorrect": 1,
    }
    response = session.get(ENDPOINT, params=params)
    return response.json()


def _write_track_info(data: Dict[str, Any], song: Song) -> None:
    logging.info(f"Writing out track info for {song}...")
    # Inject reference data so we can link the two tables together.
    data["artist_name"] = song.artist
    data["song_name"] = song.name
    # Create the path if it doesn't already exist.
    last_fm_path = DATA_PATH / "lastfm" / "tracks"
    last_fm_path.mkdir(parents=True, exist_ok=True)
    with open(last_fm_path / f"{song.artist}-{song.name.replace('/', '|')}.json", "w") as f:
        json.dump(data, f)


def _request_artist_info(session: requests.Session, mbid: str) -> Dict[str, Any]:
    params = {
        "method": "artist.getinfo",
        "api_key": API_KEY,
        "mbid": mbid,
        "format": "json",
    }
    response = session.get(ENDPOINT, params=params)
    return response


def _write_artist_info(mbid: str, artist_info: requests.Response) -> None:
    logging.info(f"Writing out artist info for {mbid}...")
    last_fm_path = DATA_PATH / "lastfm" / "artists"
    last_fm_path.mkdir(parents=True, exist_ok=True)
    with open(last_fm_path / f"{mbid}.json", "w") as f:
        json.dump(artist_info.text, f)


def main() -> None:
    _start_time = datetime.datetime.now()
    logging.info("Start downloading last.fm data...")
    conn = sqlite3.connect(DATA_PATH / "dataset.sqlite")
    songs = _get_unique_songs(conn)
    logging.info(f"Processing {len(songs)} songs...")

    # Fetch song then artist to minimise API calls.
    artists = []
    logging.info("Processing songs...")
    with requests.Session() as session:
        for song in songs:
            # First get track info
            try:
                track_info = _request_track_info(session, song)
            except Exception as e:
                logging.error(f"Unable to download data for {song} with error {type(e)} - {e}...")
            if "error" in track_info:
                logging.error(f"Unable to find song {song} with error code {track_info['error']}...")
                continue
            _write_track_info(track_info, song)

            # Record the last.fm artist mbid, if it doesn't exist assume that it is not a major artist/group (low listens).
            # I've validated this against some artists such as the `1999 Manchester United Squad`.
            artist_mbid = track_info["track"]["artist"].get("mbid")
            if artist_mbid:
                artists.append(artist_mbid)

    logging.info("Processing artists...")
    with requests.Session() as session:
        for artist in set(artists):  # Unique artist ids
            artist_info = _request_artist_info(session, artist)
            _write_artist_info(artist, artist_info)

    _end_time = datetime.datetime.now()
    logging.info(f"Finished processing in {(_end_time - _start_time).seconds}...")


if __name__ == "__main__":
    main()
