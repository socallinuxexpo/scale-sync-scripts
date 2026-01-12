#!/usr/bin/python3

#
# Copyright 2018 Southern California Linux Expo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Author:: Phil Dibowitz <phil@ipm.com>
# This is quick-n-dirty script to import a CSV export from the SCALE
# website into Guidebook. By default it'll add only what's missing, but
# can optionally update all existing sessions.
#
# It automatically setups rooms ("Locations") and tracks. It has a hard-coded
# map of colors in the Guidebook class, so if you change tracks you'll need
# to update that.
#

import click
import csv
import logging
import requests
import sys
import pytz
from datetime import datetime

DBASE_DEFAULT = "/tmp/presentation_exporter_event_1967.csv"
GUIDE_NAME = "SCaLE 23x"


class OurCSV:
    rooms = set()
    tracks = set()
    sessions = set()

    FIELD_MAPPING = {
        "tracks": "Session Track",
        "rooms": "Room/Location",
    }

    def __init__(self, dbase, logger):
        self.logger = logger
        self.sessions = self.load_csv(dbase)

    def load_csv(self, filename):
        self.logger.info("Loading CSV file")
        data = []
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
            for row in reader:
                track = row[self.FIELD_MAPPING["tracks"]]
                room = row[self.FIELD_MAPPING["rooms"]]
                if track != "":
                    self.tracks.add(track)
                if room != "":
                    self.rooms.add(room)
                data.append(row)
        return data


class GuideBook:
    URLS = {
        "guide": "https://builder.guidebook.com/open-api/v1.1/guides/",
        "tracks": "https://builder.guidebook.com/open-api/v1.1/schedule-tracks/",
        "rooms": "https://builder.guidebook.com/open-api/v1.1/locations/",
        "sessions": "https://builder.guidebook.com/open-api/v1.1/sessions/",
        "x-rooms": "https://builder.guidebook.com/api/locations/",
        "x-maps": "https://builder.guidebook.com/api/maps/",
        "x-map-regions": "https://builder.guidebook.com/api/map-regions/",
    }

    COLOR_MAP = {
        "Applied Science": "#dddddd",
        "AstriCon": "#8b4789",
        "BoFs": "#ffbc00",
        "Career Day": "#dddddd",  # Open Source Career Day
        "Cloud Native": "#638dce",  # Cloud Native Days
        "Developer": "#d65c09",
        "DevOpsDay LA": "#565448",
        "Embedded Linux": "#004a4a",
        "FOSS @ HOME": "#998876",
        "General": "#97a67a",
        "HAM Radio": "#96beef",
        "Higher Education": "#fff8dc",  # Open Source in Higher Education
        "Kernel & Low Level Systems": "#ffa200",
        "Keynote": "#d31111",
        "MySQL": "#0aaca0",
        "Next Generation": "#96f74b",  # The Next Generation
        "Observability": "#ffbc00",
        "Open Government": "#6c6c6c",
        "Open Source AI": "#ffd672",
        "PlanetNix": "#2d5d3f",
        "PostgreSQL": "#0aaca0",
        "Security": "#000000",
        "SunSecCon": "#e63946",
        "Systems & Infrastructure": "#c4c249",
        "Workshops": "#774022",
    }

    ROOM_TO_MAP_REGION = {
        "Ballroom A": {"h": 0.04, "w": 0.056, "x": 0.668, "y": 0.477},
        "Ballroom B": {"h": 0.04, "w": 0.056, "x": 0.668, "y": 0.519},
        "Ballroom C": {"h": 0.04, "w": 0.056, "x": 0.668, "y": 0.56},
        "Ballroom DE": {"h": 0.122, "w": 0.082, "x": 0.729, "y": 0.477},
        "Ballroom F": {"h": 0.04, "w": 0.056, "x": 0.816, "y": 0.56},
        "Ballroom G": {"h": 0.04, "w": 0.056, "x": 0.816, "y": 0.519},
        "Ballroom H": {"h": 0.04, "w": 0.056, "x": 0.816, "y": 0.477},
        "Check-in": {"h": 0.09, "w": 0.06, "x": 0.608, "y": 0.301},
        "Exhibit Hall": {"h": 0.141, "w": 0.209, "x": 0.675, "y": 0.189},
        "Room 101": {"h": 0.039, "w": 0.042, "x": 0.58, "y": 0.843},
        "Room 102": {"h": 0.039, "w": 0.042, "x": 0.535, "y": 0.843},
        "Room 103": {"h": 0.039, "w": 0.042, "x": 0.488, "y": 0.843},
        "Room 104": {"h": 0.039, "w": 0.042, "x": 0.443, "y": 0.843},
        "Room 105": {"h": 0.039, "w": 0.042, "x": 0.396, "y": 0.843},
        "Room 106": {"h": 0.048, "w": 0.077, "x": 0.396, "y": 0.713},
        "Room 107": {"h": 0.048, "w": 0.077, "x": 0.545, "y": 0.713},
        "Room 204": {"h": 0.042, "w": 0.03, "x": 0.231, "y": 0.836},
        "Room 205": {"h": 0.021, "w": 0.025, "x": 0.201, "y": 0.836},
        "Room 207": {"h": 0.042, "w": 0.03, "x": 0.154, "y": 0.836},
        "Room 208": {"h": 0.042, "w": 0.03, "x": 0.121, "y": 0.836},
        "Room 209": {"h": 0.021, "w": 0.025, "x": 0.079, "y": 0.858},
        "Room 210": {"h": 0.021, "w": 0.025, "x": 0.079, "y": 0.836},
        "Room 211": {"h": 0.039, "w": 0.065, "x": 0.079, "y": 0.713},
        "Room 212": {"h": 0.039, "w": 0.03, "x": 0.237, "y": 0.713},
        "Room 214": {"h": 0.039, "w": 0.03, "x": 0.273, "y": 0.713},
    }

    REGIONED_MAP = "Pasadena-Convention-Center-Map-1000-72-fs8"

    def __init__(self, logger, update, key, x_key=None):
        self.logger = logger
        self.update = update
        self.headers = {"Authorization": "JWT " + key}
        self.guide = self.get_guide()
        self.tracks = self.get_things("tracks")
        self.rooms = self.get_things("rooms")
        self.sessions = self.get_things("sessions")
        self.x_rooms = []

        if x_key:
            self.x_headers = {"Authorization": "JWT " + x_key}
            self.x_rooms = self.get_things("x-rooms")
            self.x_map_id = self.get_x_map_id()
            self.x_map_regions = self.get_things("x-map-regions")

    def get_guide(self):
        """
        We always have a single guide, and need it's IDs for most calls,
        so we request all guides, check there's only one, and then return
        it's ID.
        """
        response = requests.get(self.URLS["guide"], headers=self.headers).json()
        guide_id = None
        for guide in response["results"]:
            if guide["name"].lower() == GUIDE_NAME.lower():
                guide_id = guide["id"]
                break
        if not guide_id:
            self.logger.critical("ERROR: Could not determine guide ID")
            sys.exit(1)
        return guide_id

    def get_x_map_id(self):
        response = requests.get(
            self.URLS["x-maps"] + "?guide=%d" % self.guide,
            headers=self.x_headers,
        ).json()
        for r in response["results"]:
            if r["name"]["en-US"] == self.REGIONED_MAP:
                self.logger.debug(
                    "Found map '%s' with id=%d for guide=%d",
                    self.REGIONED_MAP,
                    r["id"],
                    self.guide,
                )
                return r["id"]
        self.logger.critical(
            "ERROR: Did not find expected map '%s' for guide %d",
            self.REGIONED_MAP,
            self.guide,
        )
        sys.exit(1)

    def get_things(self, thing):
        """
        Get the current set of <thing> from Guidebook, where <thing> is rooms,
        tracks, sessions.
        """
        msg = "Loading %s from Guidebook" % thing
        ourthings = {}
        url = self.URLS[thing] + "?guide=%d" % self.guide
        page = 1
        while url is not None:
            self.logger.info("%s (page %d)" % (msg, page))
            response = requests.get(
                url,
                headers=(
                    self.headers
                    if not thing.startswith("x-")
                    else self.x_headers
                ),
            ).json()
            self.logger.debug("Response: %s" % response)
            for ourthing in response["results"]:
                # Fallback to id for things without names (e.g. map-regions)
                name = ourthing.get("name") or ourthing.get("id")
                if isinstance(name, dict):
                    # Things retrived from the internal API
                    # (i.e. x-* things) have names that are dicts like:
                    # 'name': { 'en-US': 'Thing name' }
                    # Assume first value is what we want
                    name = list(name.values())[0]
                ourthings[name] = ourthing
            url = response["next"]
            page += 1
        self.logger.debug("Loaded %s: %s things", thing, len(ourthings))
        return ourthings

    def add_thing(self, thing, name, data, update, tid):
        """
        Implementation of adding objects to Guidebook. Wrapped by the
        functions that know how to build the data and use it.
        """
        verb = "Updating" if update else "Adding"
        self.logger.info("%s %s '%s' to Guidebook" % (verb, thing, name))
        self.logger.debug("Data: %s" % data)
        headers = self.headers if not thing.startswith("x-") else self.x_headers
        if update:
            response = requests.patch(
                self.URLS[thing] + "%d/" % tid, data=data, headers=headers
            ).json()
        else:
            response = requests.post(
                self.URLS[thing], data=data, headers=headers
            ).json()
        self.logger.debug("Response: %s" % response)
        if "id" not in response:
            self.logger.error("Failed to import.")
            self.logger.error("DATA: %s" % data)
            self.logger.error("RESPONSE: %s" % response)
            sys.exit(1)
        return response

    def add_track(self, track, update, tid):
        """
        Track-specific wrapper around add_thing()
        """
        if update and not self.update:
            return
        data = {
            "guide": self.guide,
            "name": track,
            # NOTE WELL: Guidebook cannot handle lower-case letters
            "color": self.COLOR_MAP[track].upper(),
        }
        self.tracks[track] = self.add_thing("tracks", track, data, update, tid)

    def setup_tracks(self, tracks):
        """
        Add all tracks passed in if missing.
        """
        for track in tracks:
            update = False
            tid = None
            if track in self.tracks:
                update = True
                tid = self.tracks[track]["id"]
            self.add_track(track, update, tid)

    def add_room(self, room, update, rid):
        """
        Room-specific wrapper around add_thing()
        """
        if update and not self.update:
            return
        data = {
            "guide": self.guide,
            "name": room,
            "location_type": 2,  # not google maps
        }
        self.rooms[room] = self.add_thing("rooms", room, data, update, rid)

    def setup_rooms(self, rooms):
        """
        Add all rooms passed in if missing.
        """
        for room in rooms:
            update = False
            rid = None
            if room in self.x_rooms:
                continue
            if room in self.rooms:
                update = True
                rid = self.rooms[room]["id"]
            self.add_room(room, update, rid)

    def add_x_map_region(self, map_region, update, rid, location_id):
        if update and not self.update:
            return
        name = ("map-regions-%s" % rid,)
        data = {
            "map_object": self.x_map_id,
            "location": location_id,
            "relative_x": map_region["x"],
            "relative_y": map_region["y"],
            "relative_width": map_region["w"],
            "relative_height": map_region["h"],
        }
        self.add_thing("x-map-regions", name, data, update, rid)

    def get_x_map_region_for_room(self, location_id):
        return next(
            (
                reg
                for reg in self.x_map_regions.values()
                if (
                    reg["location"] is not None
                    and "name" in reg["location"]
                    and reg["location"]["id"] == location_id
                )
            ),
            None,
        )

    def setup_x_map_regions(self):
        for room, map_region in self.ROOM_TO_MAP_REGION.items():
            if room not in self.x_rooms:
                self.logger.warning(
                    'Room "%s" does not exist in Guidebook. '
                    "Skipping map region %s",
                    room,
                    map_region,
                )
                continue
            update = False
            rid = None
            location_id = self.x_rooms[room]["id"]
            guidebook_map_region = self.get_x_map_region_for_room(location_id)
            if guidebook_map_region:
                update = True
                rid = guidebook_map_region["id"]

            if self.update:
                # Update room's Guidebook location to work the map region.
                # NOTE: Changing the type to gb-interactive hides the location
                # from the official API so it's might break other things.
                self.add_thing(
                    "x-rooms",
                    room,
                    data={"location_type": "gb-interactive"},
                    update=True,
                    tid=self.x_rooms[room]["id"],
                )

            self.add_x_map_region(map_region, update, rid, location_id)

    def to_utc(self, ts):
        loc_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M")
        pt_dt = pytz.timezone("America/Los_Angeles").localize(loc_dt)
        return pt_dt.astimezone(pytz.utc)

    def get_times(self, session):
        """
        Helper function to build times for guidebook.
        """
        d = session["Date"].split()[1]
        month, date, year = d.split("/")

        start_ts = "%s-%s-%s %s" % (year, month, date, session["Time Start"])

        end_ts = "%s-%s-%s %s" % (year, month, date, session["Time End"])
        return (self.to_utc(start_ts), self.to_utc(end_ts))

    def get_id(self, thing, session):
        """
        Get the ID for <thing> where thing is a room or track
        """
        key = OurCSV.FIELD_MAPPING[thing]
        if session[key] == "":
            return []
        self.logger.debug(
            "Thing: %s, Key: %s, Val: %s" % (thing, key, session[key])
        )
        # This is `ourlist = self.rooms` or `ourlist = self.tracks`
        ourlist = getattr(self, thing)
        self.logger.debug("List of %s's is %s" % (thing, ourlist.keys()))
        ourid = None
        if session[key] in ourlist:
            ourid = ourlist[session[key]]["id"]
        else:
            ourlist = getattr(self, "x_%s" % thing)
            ourid = ourlist[session[key]]["id"]
        return [ourid]

    def add_session(self, session, update, sid=None):
        """
        Sesssion-specific wrapper around add_thing()
        """
        if update and not self.update:
            return
        name = session["Session Title"]
        start, end = self.get_times(session)
        data = {
            "name": name,
            "start_time": start,
            "end_time": end,
            "guide": self.guide,
            "description_html": "<p>%s</p>" % session["Description"],
            "schedule_tracks": self.get_id("tracks", session),
            "locations": self.get_id("rooms", session),
            "add_to_schedule": True,
        }
        self.logger.debug("Data: %s" % data)
        self.sessions[name] = self.add_thing(
            "sessions", name, data, update, sid
        )

    def setup_sessions(self, sessions):
        """
        Add all rooms passed in if missing.
        """
        for session in sessions:
            name = session["Session Title"]
            update = False
            sid = None
            if session["Date"] == "":
                self.logger.warning("Skipping %s - no date" % name)
                continue
            if name in self.sessions:
                update = True
                sid = self.sessions[name]["id"]
            self.add_session(session, update, sid)

    def delete_sessions(self):
        self.logger.warning("Deleting all sessions")
        for session in self.sessions.values():
            self.logger.debug(
                "Deleting session %d [%s]" % (session["id"], session["name"])
            )
            response = requests.delete(
                self.URLS["sessions"] + "%d/" % session["id"],
                headers=self.headers,
            )
            self.logger.debug("Got %d" % response.status_code)
            if not (response.status_code >= 200 and response.status_code < 300):
                self.logger.error("Failed to delete")
                self.logger.error("RESPONSE: %s" % response.json())
                sys.exit(1)

    def delete_tracks(self):
        self.logger.warning("Deleting all tracks")
        for track in self.tracks.values():
            self.logger.debug(
                "Deleting track %d [%s]" % (track["id"], track["name"])
            )
            response = requests.delete(
                self.URLS["tracks"] + "%d/" % track["id"],
                headers=self.headers,
            )
            if response.status_code != 204:
                self.logger.error("Failed to delete")
                self.logger.error("RESPONSE: %s" % response.json())
                sys.exit(1)

    def delete_rooms(self):
        self.logger.warning("Deleting all rooms")
        for room in self.rooms.values():
            self.logger.debug(
                "Deleting room %d [%s]" % (room["id"], room["name"])
            )
            response = requests.delete(
                self.URLS["rooms"] + "%d/" % room["id"],
                headers=self.headers,
            )
            if response.status_code != 204:
                self.logger.error("Failed to delete")
                self.logger.error("RESPONSE: %s" % response.json())
                sys.exit(1)

    def delete_all(self):
        self.delete_sessions()
        self.delete_tracks()
        self.delete_rooms()


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--debug/--no-debug", "-d", default=False, help="Print debug messages."
)
@click.option(
    "--update/--no-update",
    "-u",
    default=False,
    help="Update existing sessions.",
)
@click.option(
    "--delete-all/--no-delete-all",
    default=False,
    help="Delete all tracks, rooms, and sessions",
)
@click.option("--csv-file", default=DBASE_DEFAULT, help="CSV file to use.")
@click.option(
    "--api-file",
    "-a",
    default="guidebook_api.txt",
    help="File to read API key from",
)
@click.option(
    "--x-api-file",
    "-x",
    default="guidebook_api_x.txt",
    help="File to read API key from",
)
def main(debug, update, delete_all, csv_file, api_file, x_api_file):
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logger = logging.getLogger("genbook")
    logger.setLevel(level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    with open(api_file, "r") as api:
        key = api.read().strip()

    try:
        with open(x_api_file, "r") as api:
            x_key = api.read().strip()
    except IOError:
        x_key = None

    if delete_all:
        print("WARNING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")  # noqa: E999
        print("This will cause any attendee who has saved any sessions")
        print("into a schedule to lose all of that work.")
        click.confirm("ARE YOU FUCKING SURE?!", abort=True)
    else:
        ourdata = OurCSV(csv_file, logger)

    ourguide = GuideBook(logger, update, key, x_key=x_key)
    if delete_all:
        ourguide.delete_all()
    else:
        ourguide.setup_tracks(ourdata.tracks)
        ourguide.setup_rooms(ourdata.rooms)
        ourguide.setup_sessions(ourdata.sessions)
        if x_key:
            ourguide.setup_x_map_regions()


if __name__ == "__main__":
    main()
