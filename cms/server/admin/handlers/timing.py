#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Read-only timing roster for the evidence recorder.

Returns each contest's effective timing and every participation's computed
actual start/stop, including delay_time and extra_time.  Authentication is a
shared bearer token, configured as [admin_web_server] timing_token; an empty
token disables the endpoint.  The route is served through the existing
/private/admin/ proxy and does not require an admin session.
"""

import hashlib
import hmac
import json

try:
    import tornado4.web as tornado_web
except ImportError:
    import tornado.web as tornado_web
from sqlalchemy.orm import selectinload

from cms import config
from cms.db import Contest, Participation
from cms.server.contest.phase_management import compute_actual_phase
from .base import BaseHandler


def _iso(value):
    """Format a naive UTC datetime as RFC 3339; None passes through."""
    if value is None:
        return None
    return value.replace(microsecond=0).isoformat() + "Z"


def _seconds(value):
    return None if value is None else int(value.total_seconds())


class TimingRosterHandler(BaseHandler):
    """Return contests and every participation's effective timing.

    Authentication is a shared bearer token, not an admin cookie.  The handler
    is read-only and must not be wrapped in @require_permission: the evidence
    API has no CMS admin session.
    """

    def get(self):
        expected = config.admin_web_server.timing_token
        if not expected:
            raise tornado_web.HTTPError(503, "timing roster is not configured")

        supplied = self.request.headers.get("Authorization", "").strip()
        if not hmac.compare_digest(supplied, f"Bearer {expected}"):
            raise tornado_web.HTTPError(401, "invalid timing token")

        now = self.timestamp
        contests = (
            self.sql_session.query(Contest)
            .options(
                selectinload(Contest.participations).selectinload(
                    Participation.user
                )
            )
            .order_by(Contest.id)
            .all()
        )

        payload = {"serverTime": _iso(now), "contests": []}
        for contest in contests:
            participations = []
            for p in sorted(contest.participations,
                            key=lambda item: item.user.username):
                phase, _, _, actual_start, actual_stop = compute_actual_phase(
                    now,
                    contest.start,
                    contest.stop,
                    contest.analysis_start if contest.analysis_enabled else None,
                    contest.analysis_stop if contest.analysis_enabled else None,
                    contest.per_user_time,
                    p.starting_time,
                    p.delay_time,
                    p.extra_time,
                )
                participations.append({
                    "username": p.user.username,
                    "startingTime": _iso(p.starting_time),
                    "delaySeconds": _seconds(p.delay_time),
                    "extraSeconds": _seconds(p.extra_time),
                    "actualStart": _iso(actual_start),
                    "actualStop": _iso(actual_stop),
                    "phase": phase,
                    "hidden": p.hidden,
                    "unrestricted": p.unrestricted,
                })

            payload["contests"].append({
                "id": contest.id,
                "name": contest.name,
                "description": contest.description,
                "start": _iso(contest.start),
                "stop": _iso(contest.stop),
                "perUserTimeSeconds": _seconds(contest.per_user_time),
                "analysisEnabled": bool(contest.analysis_enabled),
                "analysisStart": (
                    _iso(contest.analysis_start)
                    if contest.analysis_enabled else None
                ),
                "analysisStop": (
                    _iso(contest.analysis_stop)
                    if contest.analysis_enabled else None
                ),
                "participations": participations,
            })

        body = json.dumps(
            payload, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        etag = '"' + hashlib.sha256(body).hexdigest() + '"'
        if self.request.headers.get("If-None-Match") == etag:
            self.set_status(304)
            return

        self.set_header("ETag", etag)
        self.set_header("Cache-Control", "no-store")
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(body)
