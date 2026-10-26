#!/usr/bin/env python3
# Copyright 2026 The Oxia Authors
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

"""Fail unless a JaCoCo XML report covers every line and branch."""

import sys
import xml.etree.ElementTree as ET


def counter(element, kind):
    for c in element.findall("counter"):
        if c.get("type") == kind:
            return int(c.get("missed")), int(c.get("covered"))
    return 0, 0


def main(path):
    report = ET.parse(path).getroot()
    for package in report.findall("package"):
        for source in package.findall("sourcefile"):
            for line in source.findall("line"):
                missed_lines, missed_branches = int(line.get("mi")), int(line.get("mb"))
                if missed_lines or missed_branches:
                    print(
                        f"{package.get('name')}/{source.get('name')}:{line.get('nr')}: "
                        f"{missed_lines} instructions and {missed_branches} branches not covered"
                    )

    failed = False
    for kind in ("LINE", "BRANCH"):
        missed, covered = counter(report, kind)
        print(f"{kind}: {covered} of {missed + covered} covered")
        failed |= missed > 0
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
