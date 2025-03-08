# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for OpenRelik utils."""

import os
import unittest

from src.utils import create_disk_image


class TestContainerUtils(unittest.TestCase):
    """Unit test for OpenRelik container utils."""

    def test_create_disk_image(self):
        """Test create_disk_image."""
        path = "/tmp/disk.img"
        volume_name = "test"
        size = 10

        if os.path.exists(path):
            os.remove(path)

        _path = create_disk_image(path, volume_name, size)

        self.assertEqual(path, _path)


if __name__ == "__main__":
    unittest.main()
