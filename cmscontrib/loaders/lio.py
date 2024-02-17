#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
# Copyright © 2018 Pēteris Pakalns <peterispakalns@gmail.com>
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

import io
import os
import re
import pytz
import tempfile
import logging
import typst
import pathlib
import zipfile
import datetime
import subprocess
import shutil

from cms import config
from cms.db import Contest, Dataset, Task, Statement, Testcase, Manager, Attachment
from .base_loader import ContestLoader, TaskLoader
from cms import TOKEN_MODE_DISABLED, TOKEN_MODE_FINITE, TOKEN_MODE_INFINITE
from cmscommon.constants import \
    SCORE_MODE_MAX, SCORE_MODE_MAX_SUBTASK, SCORE_MODE_MAX_TOKENED_LAST
from .italy_yaml import load_yaml_from_path, make_timedelta
from datetime import timedelta

# Workaround to make new features available in python3.8 for typst library
import importlib.resources as importlib_res
import importlib_resources
setattr(importlib_res, "files", importlib_resources.files)
setattr(importlib_res, "as_file", importlib_resources.as_file)

logger = logging.getLogger(__name__)


class LioLoaderException(Exception):
    pass


def set_if_present(src_dict, trg_dict, key, conv=lambda x: x, default=None):
    if key in src_dict:
        trg_dict[key] = conv(src_dict[key])
    elif default is not None:
        trg_dict[key] = default

def to_datetime(dt):
    if isinstance(dt, datetime.datetime):
        pass
    elif isinstance(dt, str):
        dt = datetime.datetime.fromisoformat(dt)
    else:
        raise ValueError("Not a correct datetime")

    dt = dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=None)

def submission_format_convert(value):
    if isinstance(value, str):
        return [f"{value}.%l"]
    elif isinstance(value, list):
        return value

class LioTaskLoader(TaskLoader):

    short_name = "lio-task"
    description = "Latvian Informatics Olympiad task loader"

    def __init__(self, path, file_cacher):
        super().__init__(path, file_cacher)
        self.task_dir = os.path.dirname(self.path)
        self.conf = load_yaml_from_path(self.path)


    @staticmethod
    def detect(path):
        # TODO: Support auto detection
        return False


    # TODO: Read subgroup points from the yaml file when possible
    def parse_point_file(self, point_path):
        """
        Parse point file with the following format for each line
            {from group}-{till group} {points for each group} {comment}

        return ({group: points}): Dictionary of points per group
        """
        with open(point_path, "rt", encoding="utf-8") as f:
            content = [line.strip() for line in f.readlines()]
        points_per_group = dict()
        for line in content:
            vars = line.replace("-", " ").split()
            a = int(vars[0])
            b = int(vars[1])
            points = int(vars[2])
            for group in range(a, b+1):
                if group in points_per_group:
                    raise LioLoaderException("Duplicated groups in point file")
                points_per_group[group] = points
        for group in range(len(points_per_group)):
            if group not in points_per_group:
                raise LioLoaderException("Missing group from point file")
        if sum(points_per_group.values()) != 100:
            raise LioLoaderException("Points for all groups doesn't sum up to 100")
        return points_per_group

    def parse_points_and_public_groups(self, points):

        points_per_group = dict()
        public_groups = set()
        for row in points:
            groups = row["groups"]
            if isinstance(groups, int):
                groups = [groups]
            elif isinstance(groups, list):
                from_gr, to_gr = groups
                if to_gr < from_gr:
                    raise ValueError("Bad group interval")
                groups = list(range(from_gr, to_gr + 1))
            else:
                raise ValueError(f"Unparsabled groups {groups}")

            if not isinstance(row["points"], int):
                raise ValueError("Provided points are not points")

            for group in groups:
                if group in points_per_group:
                    raise LioLoaderException("Duplicated groups in point file")
                points_per_group[group] = row["points"]

            public = row.get("public", False)
            if isinstance(public, bool):
                if public:
                    public_groups.update(groups)
            elif isinstance(public, list):
                for group in public:
                    if group not in groups:
                        raise ValueError(f"Provided group number {group} not in this test group block {groups}")
                    public_groups.add(group)

        for group in range(len(points_per_group)):
            if group not in points_per_group:
                raise LioLoaderException("Missing group from point file")
        if sum(points_per_group.values()) != 100:
            raise LioLoaderException("Points for all groups doesn't sum up to 100")

        return points_per_group, public_groups


    def get_task(self, get_statement):
        task_args = {
            'name': self.conf['name'],
            'title': self.conf['title'],
        }
        name = task_args['name']

        logger.info(f"Loading parameters for task {name}")

        if get_statement:
            task_args['statements'] = {}
            for statement in self.conf.get('statements', []):
                path, lang = statement

                if pathlib.Path(path).suffix.lower() == ".typ":
                    logger.info("Compiling typst document")
                    statement_pdf = typst.compile(
                        os.path.join(self.task_dir, path),
                        root=os.path.join(self.task_dir, "../")
                    )
                    digest = self.file_cacher.put_file_content(
                        statement_pdf,
                        f"Statement for task {name} (lang: {lang})",
                    )
                else:
                    logger.info(f"Loading statement: {statement}")
                    digest = self.file_cacher.put_file_from_path(
                        os.path.join(self.task_dir, path),
                        f"Statement for task {name} (lang: {lang})",
                    )
                task_args['statements'][lang] = Statement(lang, digest)
            if task_args['statements']:
                task_args['primary_statements'] = self.conf.get('primary_statements', ["lv"])

        set_if_present(self.conf, task_args, 'submission_format', conv=submission_format_convert, default=submission_format_convert(name))

        set_if_present(self.conf, task_args, 'score_precision')

        score_mode = self.conf.get('score_mode', SCORE_MODE_MAX_TOKENED_LAST)
        if score_mode in [SCORE_MODE_MAX, SCORE_MODE_MAX_SUBTASK, SCORE_MODE_MAX_TOKENED_LAST]:
            task_args['score_mode'] = score_mode
        else:
            raise LioLoaderException("Unknown score mode provided")

        set_if_present(self.conf, task_args, 'max_submission_number', default=30)
        set_if_present(self.conf, task_args, 'max_user_test_number', default=100)
        set_if_present(self.conf, task_args, 'min_submission_interval', make_timedelta)
        set_if_present(self.conf, task_args, 'min_user_test_interval', make_timedelta)
        task_args["attachments"] = dict()

        args = {}
        args["description"] = self.conf.get("version", "Default")
        args["autojudge"] = False

        args['time_limit'] = float(self.conf.get('time_limit', 2))
        # Memory limit in MiB
        args['memory_limit'] = self.conf.get('memory_limit', 256) * 1024**2

        # Builds the parameters that depend on the task type
        args["managers"] = {}

        # By default use standard input, output
        input_filename = self.conf.get("input_filename", "")
        output_filename = self.conf.get("output_filename", "")

        # No grader support
        manager = None
        manager_name = None
        is_communication = False
        set_if_present(self.conf, args, 'task_type', default="Batch")

        if args["task_type"] == "Batch":
            manager = self.conf.get("checker")
            manager_name = "checker"
        elif args["task_type"] == "Communication":
            is_communication = True
            manager = self.conf.get("interactor")
            manager_name = "manager"
            if manager is None:
                raise ValueError("For Communication task interactor must be provided")
        else:
            raise ValueError("Unknown task type")

        if manager:
            logger.info("Compiling manager (checker or interactor)")
            manager_src = os.path.join(self.task_dir, manager)

            if is_communication:
                testlib_path = os.path.join(os.path.dirname(__file__), "lio", "interactive")
            elif config.installed:
                testlib_path = "/usr/local/include/cms"
            else:
                testlib_path = os.path.join(os.path.dirname(__file__), "polygon")

            with tempfile.TemporaryDirectory() as tmp_dir:
                # Moving to tmp directory to avoid problems with local testlib header file
                tmp_src_file = os.path.join(tmp_dir, os.path.split(manager_src)[1])
                shutil.copyfile(manager_src, tmp_src_file)
                checker_exe = os.path.join(tmp_dir, "checker")
                code = subprocess.call(["g++", "-x", "c++", "-O2", "-static",
                                        "-pipe", "-s", "-DCMS",
                                        # "-include", os.path.join(testlib_path, "testlib.h"),
                                        "-I", testlib_path,
                                        "-o", checker_exe, tmp_src_file])
                if code != 0:
                    raise LioLoaderException("Could not compile checker")
                digest = self.file_cacher.put_file_from_path(
                    checker_exe, f"Checker for task {name}"
                )
            args["managers"]["checker"] = Manager(manager_name, digest)
            evaluation_param = "comparator"
        else:
            evaluation_param = "diff"

        if "tests_groups" in self.conf:
            points_per_group, public_groups = self.parse_points_and_public_groups(self.conf["tests_groups"])
        else:
            point_file = os.path.join(self.task_dir, self.conf.get('point_file', 'punkti.txt'))
            points_per_group = self.parse_point_file(point_file)
            public_groups = self.conf.get('public_groups', [0, 1])

        args["score_type"] = self.conf.get("score_type", "GroupMin")
        max_group_digit_length = len(str(len(points_per_group) - 1))
        args["score_type_parameters"] = \
            [[points_per_group[i], f"{i:0{max_group_digit_length}}"] for i in range(len(points_per_group))]

        if is_communication:
            args["task_type_parameters"] = \
                [1,
                 "alone",
                 "std_io"]
        else:
            args["task_type_parameters"] = \
                ["alone",
                 [input_filename, output_filename],
                 evaluation_param]

        args["testcases"] = {}
        tests_per_group = [0] * len(points_per_group)
        test_zip = os.path.join(self.task_dir, self.conf.get('tests_archive', 'testi.zip'))
        with zipfile.ZipFile(test_zip) as zip:

            # Collect and organize test files from zip archive
            matcher = re.compile(r"\.(i|o)(\d+)([a-z]*)$")
            test_files = {}
            for test_filename in zip.namelist():
                if '/' in test_filename or '\\' in test_filename:
                    raise LioLoaderException("Test zip archive contains a directory")
                match = matcher.search(test_filename)
                if not match:
                    raise LioLoaderException(f"Unsupported file in test archive {name}")

                is_input = match.group(1) == 'i'
                group = int(match.group(2))
                test_in_group = match.group(3)

                if group not in test_files:
                    test_files[group] = {}
                if test_in_group not in test_files[group]:
                    test_files[group][test_in_group] = {}

                testcase = test_files[group][test_in_group]
                testcase['input' if is_input else 'output'] = test_filename

            # Extract them in correct order and update subtask list and testcases
            for group in sorted(test_files.keys()):
                for test_in_group in sorted(test_files[group].keys()):
                    tests_per_group[group] += 1
                    testcase = test_files[group][test_in_group]
                    if 'input' not in testcase or 'output' not in testcase:
                        raise LioLoaderException(f"Input or output file not found for test {group}{test_in_group}")
                    with zip.open(testcase['input'], 'r') as input_file:
                        content = io.TextIOWrapper(input_file, encoding='ascii', newline=None).read()
                        input_digest = self.file_cacher.put_file_content(
                            content.encode('ascii'),
                            f"Input {testcase['input']} for task {name}")
                    with zip.open(testcase['output'], 'r') as output_file:
                        content = io.TextIOWrapper(output_file, encoding='ascii', newline=None).read()
                        output_digest = self.file_cacher.put_file_content(
                            content.encode('ascii'),
                            f"Output {testcase['output']} for task {name}")
                    codename = f"{group:0{max_group_digit_length}}{test_in_group}"
                    args["testcases"][codename] = \
                        Testcase(codename, group in public_groups, input_digest, output_digest)

                    if args["task_type"] == "Batch" and group in [0, 1]:
                        attachemnt_input_filename = f"{name}.i{codename}"
                        task_args["attachments"][attachemnt_input_filename] = Attachment(filename=attachemnt_input_filename, digest=input_digest)
                        if group in [0]:
                            attachemnt_output_filename = f"{name}.o{codename}"
                            task_args["attachments"][attachemnt_output_filename] = Attachment(filename=attachemnt_output_filename, digest=output_digest)

        for i in range(len(tests_per_group)):
            if tests_per_group[i] == 0:
                raise LioLoaderException(f"No testcases for group {i}")

        task = Task(**task_args)
        args["task"] = task

        task.active_dataset = Dataset(**args)

        logger.info("Task parameters loaded.")
        return task


    def task_has_changed(self):
        # TODO: Detect if the task has been changed since its last import
        # With temporary files and checking if some settings
        # and/or file last modification time has changed
        return True


class LioContestLoader(ContestLoader):

    short_name = "lio-contest"
    description = "Latvian Informatics Olympiad contest loader"

    def __init__(self, path, file_cacher):
        super().__init__(path, file_cacher)
        self.contest_dir = os.path.dirname(self.path)
        self.conf = load_yaml_from_path(self.path)

    @staticmethod
    def detect(path):
        # TODO: Support auto detection
        return False

    def get_contest(self):
        args = {
            'name': self.conf['name'],
            'description': self.conf['description'],
        }

        args['allowed_localizations'] = self.conf.get('allowed_localizations', ['lv'])
        args['languages'] = self.conf.get('languages',
            ["C11 / gcc", "C++11 / g++", "Pascal / fpc", "Java / JDK",
             "Python 3 / CPython", "Go"])

        set_if_present(self.conf, args, 'score_precision')

        logger.info("Loading parameters for contest %s.", args["name"])

        # If enabled, other token mode settings must be provided through AWS
        args['token_mode'] = self.conf.get('token_mode', TOKEN_MODE_DISABLED)

        args['start'] = to_datetime(self.conf.get('start', datetime.datetime(1970, 1, 1)))
        args['stop'] = to_datetime(self.conf.get('stop', datetime.datetime(1970, 1, 1)))
        args['timezone'] = self.conf.get('timezone', 'Europe/Riga')

        set_if_present(self.conf, args, 'per_user_time', make_timedelta)
        set_if_present(self.conf, args, 'allow_registration', default=False)
        set_if_present(self.conf, args, 'max_submission_number')
        set_if_present(self.conf, args, 'max_user_test_number')
        set_if_present(self.conf, args, 'min_submission_interval', \
                       conv=make_timedelta, default=make_timedelta(5))
        set_if_present(self.conf, args, 'min_user_test_interval', \
                       conv=make_timedelta, default=make_timedelta(5))

        set_if_present(self.conf, args, 'analysis_enabled')
        set_if_present(self.conf, args, 'analysis_start', conv=to_datetime)
        set_if_present(self.conf, args, 'analysis_stop', conv=to_datetime)

        tasks = list(self.conf['tasks'].keys())

        logger.info("Contest parameters loaded.")

        return Contest(**args), tasks, []

    def contest_has_changed(self):
        # TODO: Detect if the contest has been changed since its last import
        return True

    def get_task_loader(self, taskname):
        task_yaml_path = os.path.join(taskname, 'task.yaml')
        if self.conf['tasks'][taskname] is not None:
            task_yaml_path = self.conf['tasks'][taskname].get('config', task_yaml_path)
        task_yaml_path = os.path.join(self.contest_dir, task_yaml_path)
        return LioTaskLoader(task_yaml_path, self.file_cacher)
