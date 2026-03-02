#!/usr/bin/env python3

#
# Copyright 2018-present Southern California Linux Expo
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
#
# Script to sync the website schedule to Guidebook complete with region
# mapping.
#
# By default it'll add only what's missing, but can optionally update all
# existing sessions.
#
# It automatically setups rooms ("Locations") and tracks. It has a hard-coded
# map of colors in the Guidebook class, so if you change tracks you'll need
# to update that.
#

from bs4 import BeautifulSoup
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries
from datetime import datetime
from dateutil import parser
from markdownify import markdownify as md
import click
import json
import logging
import os
import pytz
import re
import requests
import sys
import time

try:
    import xdg_base_dirs as xdg
except ImportError:
    import xdg

EVENTS_FEED = "https://www.socallinuxexpo.org/scale/23x/app"
TRACKS_FEED = "https://www.socallinuxexpo.org/api/tracks/23x"
GUIDE_NAME = "SCaLE 23x"


class StatsTracker:
    """Track statistics for additions, updates, and deletions of items."""

    def __init__(self):
        self.stats = {
            "tracks": {"added": 0, "updated": 0, "deleted": 0},
            "rooms": {"added": 0, "updated": 0, "deleted": 0},
            "sessions": {"added": 0, "updated": 0, "deleted": 0},
            "map_regions": {"added": 0, "updated": 0, "deleted": 0},
        }

    def increment(self, item_type, operation):
        """Increment counter for a given item type and operation."""
        if item_type in self.stats and operation in self.stats[item_type]:
            self.stats[item_type][operation] += 1

    def get_stats(self):
        """Return the current statistics."""
        return self.stats

    def log_stats(self, logger):
        """Log statistics summary."""
        logger.info("=" * 60)
        logger.info("SYNC STATISTICS SUMMARY")
        logger.info("=" * 60)
        for item_type, operations in self.stats.items():
            total = sum(operations.values())
            if total > 0:
                logger.info(
                    f"{item_type.upper()}: "
                    f"Added={operations['added']}, "
                    f"Updated={operations['updated']}, "
                    f"Deleted={operations['deleted']}"
                )
        logger.info("=" * 60)

    def send_to_datadog(self, logger, dryrun=False):
        """Send metrics to Datadog."""
        if dryrun:
            logger.info("[DRYRUN] Would have sent metrics to Datadog")
            return

        dd_api_key = os.getenv("DD_API_KEY")
        dd_site = os.getenv("DD_SITE", "datadoghq.com")

        if not dd_api_key:
            logger.warning(
                "DD_API_KEY not set. Skipping Datadog metrics submission."
            )
            return

        try:
            configuration = Configuration()
            configuration.api_key["apiKeyAuth"] = dd_api_key
            configuration.server_variables["site"] = dd_site

            timestamp = int(time.time())
            series = []

            for item_type, operations in self.stats.items():
                for operation, count in operations.items():
                    metric_name = f"guidebook.sync.{item_type}.{operation}"
                    series.append(
                        MetricSeries(
                            metric=metric_name,
                            type=MetricIntakeType.COUNT,
                            points=[
                                MetricPoint(
                                    timestamp=timestamp,
                                    value=float(count),
                                )
                            ],
                            tags=[f"guide:{GUIDE_NAME}"],
                        )
                    )

            if series:
                with ApiClient(configuration) as api_client:
                    api_instance = MetricsApi(api_client)
                    body = MetricPayload(
                        series=series,
                    )
                    response = api_instance.submit_metrics(body=body)
                    logger.info("Successfully sent metrics to Datadog")
                    logger.debug(f"Datadog response: {response}")
            else:
                logger.debug("No metrics to send to Datadog")

        except Exception as e:
            logger.error(f"Failed to send metrics to Datadog: {e}")


class OurJSON:
    rooms = set()
    tracks = {}
    sessions_by_name = {}
    sessions_by_nid = {}

    FIELD_MAPPING = {
        "tracks": "Track",
        "rooms": "Location",
    }

    def __init__(self, event_feed, track_feed, logger):
        self.logger = logger

        event_data = self._get_feed_data(event_feed)
        track_data = self._get_feed_data(track_feed)

        self.sessions_by_name, self.sessions_by_nid = self._load_event_json(
            event_data
        )
        self.tracks = self._load_tracks_json(track_data)

    def _get_feed_data(self, path):
        self.logger.info("Loading JSON feed from %s" % path)
        if path.startswith("http://") or path.startswith("https://"):
            response = requests.get(path)
            return response.text
        return open(path, "r").read()

    def _load_tracks_json(self, raw):
        raw = json.loads(raw)
        tracks = {}
        for track in raw:
            name = track["name"]
            tracks[name] = track["color"]
        return tracks

    def _load_event_json(self, raw):
        raw = json.loads(raw)
        data_by_name = {}
        data_by_nid = {}
        for session in raw:
            # handle leading/trailing spaces in names
            name = session["Name"].strip()
            session["Name"] = name
            room = session[self.FIELD_MAPPING["rooms"]].strip()
            if room != "":
                self.rooms.add(room)
            clean_session = {k: v.strip() for k, v in session.items()}
            if clean_session["LongAbstract"] != "":
                html = BeautifulSoup(
                    clean_session["LongAbstract"], "html.parser"
                )
                # nuke all images from the HTML because Guidebook doesn't
                # support them and will escape the tags in a way that makes
                # us forever update the sessions as different
                for img in html.find_all("img"):
                    img.decompose()
                clean_session["LongAbstract"] = str(html)
            data_by_name[name] = clean_session
            data_by_nid[session["nid"]] = clean_session
        return (data_by_name, data_by_nid)


class GuideBook:
    URLS = {
        "guide": "https://builder.guidebook.com/open-api/v1.1/guides/",
        "tracks": "https://builder.guidebook.com/open-api/v1.1/schedule-tracks/",
        "rooms": "https://builder.guidebook.com/open-api/v1.1/locations/",
        "sessions": "https://builder.guidebook.com/open-api/v1.1/sessions/",
        "x-rooms": "https://builder.guidebook.com/api/locations/",
        "x-maps": "https://builder.guidebook.com/api/maps/",
        "x-map-regions": "https://builder.guidebook.com/api/map-regions/",
        "publish": "https://builder.guidebook.com/api/guides/{guide}/publish/",
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

    def __init__(
        self,
        logger,
        update,
        dryrun,
        max_deletes,
        key,
        stats_tracker,
        x_key=None,
    ):
        self.logger = logger
        self.update = update
        self.dryrun = dryrun
        self.max_deletes = max_deletes
        self.stats = stats_tracker
        self.headers = {"Authorization": "JWT " + key}
        self.guide = self.get_guide()
        self.tracks = self.get_things("tracks")
        self.rooms = self.get_things("rooms")
        self.sessions_by_nid = self.get_things("sessions", "import_id")
        self.sessions_by_name = {
            s["name"]: s for s in self.sessions_by_nid.values()
        }
        for nid, session in self.sessions_by_nid.items():
            assert nid == session["import_id"]
        self.x_rooms = []
        self.nids_to_delete = []

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

    def get_things(self, thing, key="name"):
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
                name = ourthing.get(key) or ourthing.get("id")
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
        if self.dryrun:
            self.logger.info(
                "[DRYRUN] Would have: %s %s '%s' to Guidebook"
                % (verb, thing, name)
            )
            return

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

    def add_track(self, track, color, update, tid):
        """
        Track-specific wrapper around add_thing()
        """
        if update and not self.update:
            return
        data = {
            "guide": self.guide,
            "name": track,
            # NOTE WELL: Guidebook cannot handle lower-case letters
            "color": color,
        }
        newinfo = self.add_thing("tracks", track, data, update, tid)
        if not self.dryrun:
            self.tracks[track] = newinfo
        operation = "updated" if update else "added"
        self.stats.increment("tracks", operation)

    def setup_tracks(self, tracks):
        """
        Add all tracks passed in if missing.
        """
        for track, color in tracks.items():
            # Guidebook only deals in upper-case colors, so we must match
            color = color.upper()
            update = False
            tid = None
            if track in self.tracks:
                orig = self.tracks[track]
                # the only "info" about a track is the color (the name is
                # our primary key), so if the color is correct, it's up to date.
                if orig["color"] == color:
                    self.logger.debug(
                        "Track '%s' exists in Guidebook and has correct color"
                        " %s. No update needed.",
                        track,
                        color,
                    )
                    continue
                update = True
                tid = self.tracks[track]["id"]
            self.add_track(track, color, update, tid)

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
        operation = "updated" if update else "added"
        self.stats.increment("rooms", operation)

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
                orig = self.rooms[room]
                # Rooms can't really change, but just in case, we'll
                # check it's location type is correct.
                if orig["location_type"] == 2:
                    self.logger.debug(
                        "Room '%s' exists in Guidebook and has correct"
                        " location_type. No update needed.",
                        room,
                    )
                    continue
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
        operation = "updated" if update else "added"
        self.stats.increment("map_regions", operation)

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

    def x_map_region_needs_update(
        self, map_region, original_map_region, location_id
    ):
        """
        Compare the new map region data to the original map region data, and
        return True if we need to update.
        """
        if original_map_region["location"]["id"] != location_id:
            self.logger.info(
                "Map region needs update because location changed: %s != %s",
                original_map_region["location"]["id"],
                location_id,
            )
            return True

        fields_to_check = [
            ("relative_x", map_region["x"]),
            ("relative_y", map_region["y"]),
            ("relative_width", map_region["w"]),
            ("relative_height", map_region["h"]),
        ]

        for field_name, new_value in fields_to_check:
            original_value = original_map_region[field_name]
            if new_value != original_value:
                self.logger.info(
                    "Map region needs update because '%s' changed: %s != %s",
                    field_name,
                    new_value,
                    original_value,
                )
                return True

        return False

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
                # Check if the map region actually needs updating
                if not self.x_map_region_needs_update(
                    map_region, guidebook_map_region, location_id
                ):
                    self.logger.debug(
                        "Map region for room '%s' exists in Guidebook and has "
                        "correct coordinates. No update needed.",
                        room,
                    )
                    continue
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

    def to_utc(self, ts, fmt):
        loc_dt = datetime.strptime(ts, fmt)
        if not fmt.endswith("%z"):
            pt_dt = pytz.timezone("America/Los_Angeles").localize(loc_dt)
        else:
            pt_dt = loc_dt
        return pt_dt.astimezone(pytz.utc).isoformat(timespec="seconds")

    def get_times(self, session):
        """
        Helper function to build times for guidebook.
        """

        fmt = "%Y-%m-%dT%H:%M:%S%z"
        start_ts = session["StartTime"]
        end_ts = session["EndTime"]
        return (self.to_utc(start_ts, fmt), self.to_utc(end_ts, fmt))

    def get_id(self, thing, session):
        """
        Get the ID for <thing> where thing is a room or track
        """
        key = OurJSON.FIELD_MAPPING[thing]
        if session[key] == "":
            return []
        self.logger.debug(
            "Thing: %s, Key: %s, Val: %s" % (thing, key, session[key])
        )
        # This is `ourlist = self.rooms` or `ourlist = self.tracks`
        ourlist = getattr(self, thing)
        self.logger.debug("List of %s's is %s" % (thing, ourlist.keys()))
        ourid = None
        if session[key] in ourlist and ourlist[session[key]] is not None:
            ourid = ourlist[session[key]]["id"]
        else:
            ourlist = getattr(self, "x_%s" % thing)
            ourid = ourlist[session[key]]["id"]
        return [ourid]

    def add_session(self, session, original_session=None):
        """
        Sesssion-specific wrapper around add_thing()
        """
        if original_session is not None and not self.update:
            return
        name = session["Name"]
        start, end = self.get_times(session)
        spkr_line = (
            f'<p><strong>Speakers</strong>: {session["Speakers"]}</strong></p>'
        )
        assembled_desc = spkr_line + session["LongAbstract"]
        data = {
            "name": name,
            "start_time": start,
            "end_time": end,
            "guide": self.guide,
            "description_html": assembled_desc,
            "schedule_tracks": self.get_id("tracks", session),
            "locations": self.get_id("rooms", session),
            "add_to_schedule": True,
            "import_id": session["nid"],
        }
        update = False
        sid = None
        if original_session is not None:
            if not self.session_needs_update(data, original_session):
                self.logger.debug("Session '%s' does not need update" % name)
                return
            update = True
            sid = original_session["id"]

        self.logger.debug("Data: %s" % data)
        s = self.add_thing("sessions", name, data, update, sid)
        self.sessions_by_nid[session["nid"]] = s
        self.sessions_by_name[name] = s
        operation = "updated" if update else "added"
        self.stats.increment("sessions", operation)

    def normalize_html(self, html):
        """
        The HTML supported by Drupal vs Guidebook is different and
        GB normalizes it upon import, so we can get in a state where
        we always detect a difference.

        Stripping HTML is lossy, so instead we convert to MD and compare
        that which gives us a lot of information about formatting without
        being sensitive to exact HTML.
        """

        markdown = md(html)
        # Normalize whitespace and quotes
        markdown = markdown.replace("\u2018", "'").replace("\u2019", "'")
        markdown = markdown.replace("\u201c", '"').replace("\u201d", '"')
        # collapse whitespace
        markdown = " ".join(markdown.split())
        return markdown

    def normalize_time(self, time_str):
        n = time_str.replace("+0000", "+00:00")
        n = parser.isoparse(n)
        n = n.astimezone(pytz.utc)
        return n

    def session_needs_update(self, new_data, original_session):
        """
        Compare the new session data to the original session data, and return
        True if we need to update. This is needed because some fields (e.g.
        description) are not updated if they haven't changed, and we want to
        avoid unnecessary updates.
        """
        all_keys = [
            "name",
            "start_time",
            "end_time",
            "description_html",
            "schedule_tracks",
            "locations",
        ]
        for key in all_keys:
            if "time" in key:
                a = self.normalize_time(original_session[key])
                b = self.normalize_time(new_data[key])
            elif "html" in key:
                a = self.normalize_html(original_session[key])
                b = self.normalize_html(new_data[key])
            else:
                a = original_session[key]
                b = new_data[key]
            if a != b:
                self.logger.info(
                    "Session '%s' needs update because '%s' changed: '%s' !="
                    " '%s'",
                    new_data["name"],
                    key,
                    a,
                    b,
                )
                return True

        return False

    def backfill_session_nids(self, sessions_by_name, sessions_by_nid):
        """
        We didn't always have a unique identifier, and this will backfill
        missing ones. Probably can be nuked by 24X.
        """
        for name, info in self.sessions_by_name.items():
            if info["import_id"] is None:
                if name in sessions_by_name.keys():
                    self.logger.info(
                        "Adding NID %s to session '%s'",
                        info["import_id"],
                        name,
                    )
                    session = sessions_by_name[name]
                    update = True
                    sid = info["id"]
                    self.add_session(session, update, sid, True)
                else:
                    self.logger.warning(
                        "Session '%s' exists in Guidebook, but has no NID,"
                        " and we cannot find the name in our data. Deleting it."
                        % name,
                    )
                    self.delete_session(info)
            else:
                nid = info["import_id"]
                if nid not in sessions_by_nid.keys():
                    self.logger.warning(
                        "Session '%s' with NID %s exists in Guidebook, but we"
                        " cannot find it in our data. Adding it to the delete"
                        " list." % (name, nid)
                    )
                    self.nids_to_delete.append(nid)

    def setup_sessions(self, sessions_by_name, sessions_by_nid):
        """
        Add all rooms passed in if missing.
        """

        # First, make sure we have NIDs for all sessions in Guidebook
        self.backfill_session_nids(sessions_by_name, sessions_by_nid)

        # now loop through pass in sessions, and add/update as needed
        for nid, session in sessions_by_nid.items():
            original_session = None
            name = session["Name"]
            if session["StartTime"] == "":
                self.logger.warning("Skipping %s - no date" % name)
                continue
            if nid in self.sessions_by_nid:
                original_session = self.sessions_by_nid[nid]
            self.add_session(session, original_session)

        # Clean up sessions that should be deleted
        num_deletes = len(self.nids_to_delete)
        if num_deletes == 0:
            return

        if num_deletes > self.max_deletes:
            self.logger.warning(
                "Number of sessions to delete (%d) exceeds the max threshold"
                " (%d). Not deleting any sessions.",
                num_deletes,
                self.max_deletes,
            )
        else:
            self.logger.warning(
                "Deleting %d sessions that are no longer in our data",
                num_deletes,
            )
            for nid in self.nids_to_delete:
                session = self.sessions_by_nid[nid]
                self.delete_session(session)

    def delete_session(self, session):
        """
        Delete a session. Unlike "add" functions, this takes the object
        from the Guidebook API, not our data.
        """
        if self.dryrun:
            self.logger.info(
                "[DRYRUN] Would have deleted session '%s' from Guidebook"
                % session["name"]
            )
            return

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
        self.stats.increment("sessions", "deleted")

    def delete_sessions(self):
        self.logger.warning("Deleting all sessions")
        for session in self.sessions_by_nid.values():
            self.delete_session(session)

    def delete_track(self, track):
        """
        Delete a track. Unlike "add" functions, this takes the object
        from the Guidebook API, not our data.
        """
        if self.dryrun:
            self.logger.info(
                "[DRYRUN] Would have deleted track '%s' from Guidebook"
                % track["name"]
            )
            return

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
        self.stats.increment("tracks", "deleted")

    def delete_tracks(self):
        self.logger.warning("Deleting all tracks")
        for track in self.tracks.values():
            self.delete_track(track)

    def delete_room(self, room):
        """
        Delete a room. Unlike "add" functions, this takes the object
        from the Guidebook API, not our data.
        """
        if self.dryrun:
            self.logger.info(
                "[DRYRUN] Would have deleted room '%s' from Guidebook"
                % room["name"]
            )
            return

        self.logger.debug("Deleting room %d [%s]" % (room["id"], room["name"]))
        response = requests.delete(
            self.URLS["rooms"] + "%d/" % room["id"],
            headers=self.headers,
        )
        if response.status_code != 204:
            self.logger.error("Failed to delete")
            self.logger.error("RESPONSE: %s" % response.json())
            sys.exit(1)
        self.stats.increment("rooms", "deleted")

    def delete_rooms(self):
        self.logger.warning("Deleting all rooms")
        for room in self.rooms.values():
            self.delete_room(room)

    def delete_all(self):
        self.delete_sessions()
        self.delete_tracks()
        self.delete_rooms()

    def publish_updates(self):
        """
        Publish pending updates. This is an internal/unpublished API, and
        this may not be identical to the "publish" button in the Guidebook
        builder. However, it does publish all pending session data at a minimum.
        """
        if self.dryrun:
            self.logger.info("[DRYRUN] Would have published pending updates.")
            return

        self.logger.info("Publishing changes")
        response = requests.post(
            self.URLS["publish"].format(guide=self.guide),
            headers=self.x_headers,
        )

        if response.status_code == 202:
            self.logger.debug("Publish accepted")
            return

        if response.status_code == 403:
            resp_text = response.text.lower()
            if "no new content" in resp_text:
                self.logger.debug("No changes to publish")
                return
            elif "currently publishing" in resp_text:
                self.logger.debug("Guidebook is already publishing")
                return

        self.logger.error("Failed to publish")
        self.logger.error("Status: %s" % response.status_code)
        self.logger.error("Body: %s" % response.text)
        sys.exit(1)


def _get_token(fname, ename, logger):
    env_token = os.getenv(ename)
    if env_token is not None:
        return env_token.strip()
    for dir in xdg.xdg_config_dirs():
        api_file = os.path.join(dir, fname)
        if os.path.isfile(api_file):
            logger.debug("Using %s from %s" % (ename, api_file))
            return open(api_file, "r").read().strip()


def get_tokens(logger):
    key = _get_token("guidebook_api_token", "GUIDEBOOK_API_TOKEN", logger)
    if not key:
        logger.critical("No API file specified. See help for details.")
        sys.exit(1)
    x_key = _get_token("guidebook_jwt_token", "GUIDEBOOK_JWT_TOKEN", logger)
    return (key, x_key)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--debug/--no-debug", "-d", default=False, help="Print debug messages."
)
@click.option(
    "--update/--no-update",
    "-u",
    default=True,
    help="Update existing sessions. Does so idempotently, defaults to True.",
)
@click.option(
    "--delete-all/--no-delete-all",
    default=False,
    help="Delete all tracks, rooms, and sessions",
)
@click.option(
    "--event-feed",
    metavar="FILE_OR_URL",
    default=EVENTS_FEED,
    help="JSON file or http(s) URL to JSON data.",
)
@click.option(
    "--track-feed",
    metavar="FILE_OR_URL",
    default=TRACKS_FEED,
    help="JSON file or http(s) URL to track data.",
)
@click.option(
    "--dryrun/--no-dryrun",
    "-n",
    default=False,
    help="Don't actually make any changes to Guidebook.",
)
@click.option(
    "--max-deletes",
    default=0,
    help="Max number of sessions to delete when syncing. Zero will not"
    " delete any sessions. Ignored if --delete-all is used.",
)
def main(
    debug, update, delete_all, event_feed, track_feed, dryrun, max_deletes
):
    """
    Sync the schedule data from our website to Guidebook.

    AUTHENTICATION

    The Guidebook API token must be provided either via the GUIDEBOOK_API_TOKEN
    environment variable, or via a file named 'guidebook_api_token' located in
    one of the standard XDG config directories (e.g. ~/.config/).

    Optionally, a Guidebook JWT token may be provided via the
    GUIDEBOOK_JWT_TOKEN environment variable or a file named
    'guidebook_jwt_token' located in one of the standard XDG config
    directories. This token is needed for certain operations such as setting up
    X Map regions and publishing.
    """
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logger = logging.getLogger("genbook")
    logger.setLevel(level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    key, x_key = get_tokens(logger)
    stats_tracker = StatsTracker()

    if delete_all:
        print("WARNING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")  # noqa: E999
        print("This will cause any attendee who has saved any sessions")
        print("into a schedule to lose all of that work.")
        click.confirm("ARE YOU FUCKING SURE?!", abort=True)
    else:
        ourdata = OurJSON(event_feed, track_feed, logger)

    ourguide = GuideBook(
        logger, update, dryrun, max_deletes, key, stats_tracker, x_key=x_key
    )
    if delete_all:
        ourguide.delete_all()
    else:
        ourguide.setup_tracks(ourdata.tracks)
        ourguide.setup_rooms(ourdata.rooms)
        ourguide.setup_sessions(
            ourdata.sessions_by_name, ourdata.sessions_by_nid
        )
        if x_key:
            ourguide.setup_x_map_regions()
            # unclear exactly when this is needed.
            ourguide.publish_updates()

    # Log and send statistics
    stats_tracker.log_stats(logger)
    stats_tracker.send_to_datadog(logger, dryrun=dryrun)


if __name__ == "__main__":
    main()
