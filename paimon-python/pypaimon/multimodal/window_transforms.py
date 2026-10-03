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

"""Column transforms for materialized training windows."""

import io


def to_tensor(values, dtype=None):
    """Convert numeric window values to a tensor with a leading time axis.

    Use ``partial(to_tensor, dtype=torch.float32)`` from ``functools``
    to choose a dtype.
    This conversion does not normalize values.
    """
    import torch

    result = torch.as_tensor(values, dtype=dtype)
    if result.ndim == 0:
        raise ValueError("Window values must have a time axis.")
    return result


def images_to_tensor(values, return_uint8=False):
    """Decode a non-empty sequence of image bytes to a TCHW tensor.

    Apply EXIF orientation, expand palettes to RGB/RGBA, and preserve grayscale
    as one channel. Eight-bit pixels become float32 in [0, 1], or stay uint8
    with ``return_uint8=True``.
    Higher-bit-depth pixels always become float32 in their original units.
    All decoded frames must have the same shape.
    """
    import torch

    if not isinstance(return_uint8, bool):
        raise TypeError("return_uint8 must be a boolean.")
    frames = []
    for payload in values:
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ValueError("Image window values must contain image bytes.")
        frame = _image_array_to_tensor(_decode_image(payload), return_uint8)
        if frames and frame.shape != frames[0].shape:
            raise ValueError("Image window frames must have the same shape.")
        frames.append(frame)
    if not frames:
        raise ValueError("Image window must contain at least one frame.")
    return torch.stack(frames)


def _decode_image(payload):
    import numpy as np
    try:
        from PIL import Image, ImageOps
    except ImportError as error:
        raise ImportError(
            "Image decoding requires Pillow; install 'pypaimon[torch]' "
            "or Pillow.") from error

    with Image.open(io.BytesIO(payload)) as image:
        image = ImageOps.exif_transpose(image)
        if image.mode == "P":
            image = image.convert("RGBA" if "transparency" in image.info else "RGB")
        array = np.array(image, copy=True)
    if array.ndim == 2:
        array = array[:, :, None]
    return array


def _image_array_to_tensor(array, return_uint8=False):
    import numpy as np
    import torch

    normalize = array.dtype == np.uint8
    tensor = torch.from_numpy(array).permute(2, 0, 1)
    if normalize and return_uint8:
        return tensor
    tensor = tensor.float()
    return tensor.div_(255) if normalize else tensor
