#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
# Copyright © 2015-2018 Stefano Maggiolo <s.maggiolo@gmail.com>
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

"""Utility to submit a solution for a user.

"""

import csv
import argparse
import logging
import sys
import typing

from typing import Iterable, Optional, Set

import gevent

from cms import ServiceCoord
from cms.db import SessionGen, Submission
from cms.db.submission import SubmissionResult
from cms.io import RemoteServiceClient
from cms.service.EvaluationService import EvaluationService

logger = logging.getLogger(__name__)

class WithRemoteServiceClient:
    def __init__(self):
        self.rs = RemoteServiceClient(ServiceCoord("EvaluationService", 0))

    def __enter__(self) -> EvaluationService:
        self.rs.connect()
        return typing.cast(EvaluationService, self.rs)

    def __exit__(self, *args):
        self.rs.disconnect()


def maybe_send_notification(submission_id):
    """Non-blocking attempt to notify a running ES of the submission"""
    rs = RemoteServiceClient(ServiceCoord("EvaluationService", 0))
    rs.connect()
    rs.new_submission(submission_id=submission_id)
    rs.disconnect()

def retrieve_all_submissions(session, submission_ids: Set[int]) -> Iterable[Submission]:
    submissions = session.query(Submission)\
        .filter(Submission.id.in_(submission_ids)).all()

    if len(submissions) != len(submission_ids):
        logging.warn("All submissions couldn't be found!")
        raise ValueError()

    return submissions


def main():
    """Parse arguments and launch process.

    return (int): exit code of the program.

    """
    parser = argparse.ArgumentParser(
        description="Recompiles and reevaluates submissions and collects execution and memory statistics")
    parser.add_argument("outfile", type=str,
                        help="path to csv file to write statistics to"
                        "Suffix 'metric_name.csv' will be appended to provided path")
    parser.add_argument("submission_ids", nargs="*", type=int,
                        help="Submission id numbers to reevaluate")
    parser.add_argument("--comment", action="store_true", help="Test all submissions with comment")
    parser.add_argument("-c", "--compile", action="store_true",
                        help="Should submission be recompiled each time")
    parser.add_argument("-n", "--count", type=int, default=1,
                        help="How many times submissions should be reevaluated")
    args = parser.parse_args()

    submission_ids = set(args.submission_ids)

    if args.comment:
        with SessionGen() as session:
            submissions = session.query(Submission)\
                .filter(Submission.comment != "").all()
            for submission in submissions:
                submission_ids.add(submission.id)


    statistics = dict()

    level = "compilation" if args.compile else 'evaluation'

    with WithRemoteServiceClient() as evaluation_service:

        with SessionGen() as session:
            submissions = retrieve_all_submissions(session, submission_ids)
            for submission in submissions:
                logging.info(f"Invalidating submission {submission.id}")
                evaluation_service.invalidate_submission(submission_id=submission.id, level=level)
                statistics[submission.id] = {
                    "task": submission.task.name,
                    "status": "OK",
                    "language": submission.language,
                    "comment": submission.short_comment,
                    "time": [],
                    "memory": [],
                    "score": [],
                    "public_score": [],
                    "compilation_time": [],
                    "compilation_memory": [],
                }

        while len(submission_ids) > 0:
            gevent.sleep(0.1)
            with SessionGen() as session:
                submissions = retrieve_all_submissions(session, submission_ids)
                for submission in submissions:
                    result: Optional[SubmissionResult] = submission.get_result()
                    if not result:
                        continue
                    status = result.get_status()

                    if status == SubmissionResult.COMPILING:
                        continue
                    elif status ==  SubmissionResult.COMPILATION_FAILED:
                        submission_ids.remove(submission.id)
                        statistics[submission.id]["status"] = "CE"
                    elif status== SubmissionResult.EVALUATING:
                        continue
                    elif status == SubmissionResult.SCORING:
                        continue
                    elif status == SubmissionResult.SCORED:
                        time, memory = result.get_max_evaluation_resources()
                        statistics[submission.id]["time"].append(
                            time
                        )
                        statistics[submission.id]["memory"].append(
                            memory
                        )
                        statistics[submission.id]["score"].append(
                            result.score
                        )
                        statistics[submission.id]["public_score"].append(
                            result.public_score
                        )
                        statistics[submission.id]["compilation_time"].append(
                            result.compilation_time
                        )
                        statistics[submission.id]["compilation_memory"].append(
                            result.compilation_memory
                        )
                        evaluations = len(statistics[submission.id]["time"])
                        if evaluations >= args.count:
                            logging.info(f"Evaluated {submission.id}")
                            submission_ids.remove(submission.id)
                        else:
                            logging.info(f"Invalidating submission {submission.id} | {evaluations}/{args.count}")
                            evaluation_service.invalidate_submission(submission_id=submission.id, level=level)
                    else:
                        assert(False)

        with open(args.outfile, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "task",
                    "status",
                    "language",
                    "comment",
                    "metric",
                ]
            )
            for id, stats in statistics.items():
                for key in ["time", "memory", "score", "public_score", "compilation_time", "compilation_memory"]:
                    writer.writerow(
                        [
                            id,
                            stats['task'],
                            stats['status'],
                            stats['language'],
                            stats['comment'],
                            key,
                            *stats[key]
                        ]
                    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
