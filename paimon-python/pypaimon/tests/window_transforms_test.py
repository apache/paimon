# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import io
import unittest
from functools import partial
from unittest.mock import patch

import numpy as np
import torch
from PIL import Image

from pypaimon.multimodal.window_transforms import images_to_tensor, to_tensor


def _png(array, **kwargs):
    buffer = io.BytesIO()
    Image.fromarray(array).save(buffer, format="PNG", **kwargs)
    return buffer.getvalue()


class WindowTransformsTest(unittest.TestCase):

    def test_numeric_values_keep_time_axis_and_explicit_dtype(self):
        convert = partial(to_tensor, dtype=torch.float64)
        values = convert([[1, 2], [3, 4]])
        self.assertEqual((2, 2), tuple(values.shape))
        self.assertEqual(torch.float64, values.dtype)
        self.assertEqual([1, 2], to_tensor([1, 2]).tolist())
        with self.assertRaisesRegex(ValueError, "time axis"):
            to_tensor(1)

    def test_rgb_and_gray_pixels_and_time_axis(self):
        rgb = _png(np.full((2, 3, 3), 128, dtype=np.uint8))
        values = images_to_tensor([rgb, rgb])
        self.assertEqual((2, 3, 2, 3), tuple(values.shape))
        self.assertEqual(torch.float32, values.dtype)
        torch.testing.assert_close(values, torch.full_like(values, 128 / 255))
        raw = images_to_tensor([rgb], return_uint8=True)
        self.assertEqual(torch.uint8, raw.dtype)
        self.assertEqual(128, raw[0, 0, 0, 0].item())
        gray = _png(np.array([[0, 255]], dtype=np.uint8))
        self.assertEqual([[[[0., 1.]]]], images_to_tensor([gray]).tolist())

    def test_high_bit_depth_keeps_native_units(self):
        payload = _png(np.array([[0, 1024, 65535]], dtype=np.uint16))
        for return_uint8 in (False, True):
            values = images_to_tensor([payload], return_uint8=return_uint8)
            self.assertEqual(torch.float32, values.dtype)
            self.assertEqual([[[[0., 1024., 65535.]]]], values.tolist())

    def test_exif_orientation_is_applied_before_stacking(self):
        exif = Image.Exif()
        exif[274] = 6
        payload = _png(np.array([[1, 2], [3, 4], [5, 6]], dtype=np.uint8),
                       exif=exif)
        values = images_to_tensor([payload], return_uint8=True)
        self.assertEqual([[[[5, 3, 1], [6, 4, 2]]]], values.tolist())

    def test_invalid_image_inputs_fail_clearly(self):
        for values in ([], [None], ["not bytes"]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                images_to_tensor(values)
        with self.assertRaises(OSError):
            images_to_tensor([b"not an image"])
        with self.assertRaises(TypeError):
            images_to_tensor([b"unused"], return_uint8=1)
        with self.assertRaises(ValueError):
            images_to_tensor([_png(np.zeros((2, 2), dtype=np.uint8)),
                              _png(np.zeros((3, 2), dtype=np.uint8))])

    def test_image_import_errors_identify_the_failing_dependency(self):
        from pypaimon.multimodal.lerobot.dataset import _image_tensor

        payload = _png(np.zeros((2, 3, 3), dtype=np.uint8))
        feature = {"dtype": "image", "shape": (2, 3, 3)}
        readers = (lambda: images_to_tensor([payload]),
                   lambda: _image_tensor(payload, feature))
        for read in readers:
            with patch.dict("sys.modules", {"PIL": None}):
                with self.assertRaisesRegex(ImportError, "requires Pillow"):
                    read()
            with patch.dict("sys.modules", {"numpy": None}):
                with self.assertRaises(ImportError) as raised:
                    read()
                self.assertEqual("numpy", raised.exception.name)
            failure = ImportError("decoder plugin unavailable")
            with patch.object(Image, "open", side_effect=failure):
                with self.assertRaises(ImportError) as raised:
                    read()
                self.assertIs(failure, raised.exception)


if __name__ == "__main__":
    unittest.main()
