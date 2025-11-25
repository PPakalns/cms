#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
# Copyright © 2023 Pēteris Pakalns <peterispakalns@gmail.com>
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

"""Event service processes different events in the system and passes
information about them to specified EventExecutors.

EventExecutors could respond to events by sending chat messages, etc.
"""

from abc import abstractmethod
import logging
from datetime import timedelta

from cms.conf import config
from cms.io import Executor, TriggeredService, rpc_method
from cms.io.priorityqueue import QueueItem
from cms.plugin import plugin_list

logger = logging.getLogger(__name__)


class EventOperation(QueueItem):
    """The operation for different kind of cms events."""

    QUESTION_NEW = "question_new"
    QUESTION_REPLIED = "question_replied"
    QUESTION_CLAIMED = "question_claimed"
    QUESTION_IGNORED = "question_ignored"
    ANNOUNCEMENT_NEW = "announcement_new"
    ANNOUNCEMENT_DELETED = "announcement_deleted"
    REFRESH = "refresh"

    def __init__(self, type, **data):
        self.type = type
        self.data = data

    def __str__(self):
        return "event %s" % (self.type)


class EventService(TriggeredService):
    """Evaluation service."""

    # Executor refresh
    EXECUTOR_REFRESH = timedelta(seconds=59)

    def __init__(self, shard):
        super().__init__(shard)

        for cls in plugin_list("cms.service.event_handlers"):
            if config.event_service is None:
                break

            if executor_config := config.event_service.event_service_handlers.get(
                cls.codename()
            ):
                print(executor_config)
                handler = cls(executor_config)
                logger.info(f"Added event executor {handler.codename()}")
                self.add_executor(handler)

        if self._executors:
            self.add_timeout(self.sweep_executors,
                             None,
                             EventService.EXECUTOR_REFRESH.total_seconds(),
                             immediately=True)
        else:
            logger.warning("No executor added for EventService")

    def sweep_executors(self):
        self.enqueue(EventOperation(EventOperation.REFRESH))

    @rpc_method
    def question_new(self, question_id: int):
        """Question was created.

        question_id (int): the id of the question.
        """
        self.enqueue(
            EventOperation(EventOperation.QUESTION_NEW, question_id=question_id)
        )

    @rpc_method
    def question_replied(self, question_id: int):
        """Question was replied.

        question_id (int): the id of the question.
        """
        self.enqueue(
            EventOperation(EventOperation.QUESTION_REPLIED, question_id=question_id)
        )

    @rpc_method
    def question_ignored(self, question_id: int):
        """Question was ignored.

        question_id (int): the id of the question.
        """
        self.enqueue(
            EventOperation(EventOperation.QUESTION_IGNORED, question_id=question_id)
        )

    @rpc_method
    def question_claimed(self, question_id: int):
        """Question was claimed.

        question_id (int): the id of the question.
        """
        self.enqueue(
            EventOperation(EventOperation.QUESTION_CLAIMED, question_id=question_id)
        )

    @rpc_method
    def announcement_new(self, announcement_id: int):
        """New announcement was posted

        announcement_id (int): the id of the announcement.
        """
        self.enqueue(
            EventOperation(
                EventOperation.ANNOUNCEMENT_NEW, announcement_id=announcement_id
            )
        )

    @rpc_method
    def announcement_deleted(self, announcement_id: int, contest_id: int):
        """Announcement was deleted

        announcement_id (int): the id of the announcement.
        contest_id (int): the id of the contest.
        """
        self.enqueue(
            EventOperation(
                EventOperation.ANNOUNCEMENT_DELETED,
                announcement_id=announcement_id,
                contest_id=contest_id,
            )
        )


class EventExecutor(Executor[EventOperation]):
    @staticmethod
    @abstractmethod
    def codename() -> str:
        return ""

