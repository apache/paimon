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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared deterministic ACT model, trainer, and window plan for benchmarks."""

import gc
import hashlib
import json
import math
import random
import time
import tracemalloc
from dataclasses import asdict, dataclass
from io import BytesIO

import numpy as np
import torch
import torch.nn.functional as functional
from PIL import Image
from torch.utils.data import default_collate


CAMERA_KEYS = (
    "observation.images.front",
    "observation.images.left_wrist",
    "observation.images.right_wrist",
)


@dataclass(frozen=True)
class BenchmarkConfig:
    """Immutable model, sampling, training, and measurement parameters.

    Every backend reconstructs this configuration from the resolved experiment
    so tensor shapes, optimizer behavior, random seeds, and metric boundaries
    remain comparable.
    """

    seed: int = 20260825
    action_horizon: int = 32
    batch_size: int = 2
    optimizer_steps: int = 2
    image_height: int = 64
    image_width: int = 80
    learning_rate: float = 1e-4
    weight_decay: float = 1e-4
    warmup_batches: int = 1
    timed_batches: int = 32
    fetch_batches: int = 8
    rounds: int = 3

    def __post_init__(self):
        positive_ints = (
            "action_horizon",
            "batch_size",
            "optimizer_steps",
            "image_height",
            "image_width",
            "warmup_batches",
            "timed_batches",
            "fetch_batches",
        )
        for name in positive_ints:
            value = getattr(self, name)
            if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or value <= 0):
                raise ValueError("%s must be a positive int." % name)
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValueError("seed must be an int.")
        if isinstance(self.rounds, bool) or not isinstance(self.rounds, int):
            raise ValueError("rounds must be an int.")
        if self.rounds < 3:
            raise ValueError("rounds must be at least 3.")
        if self.learning_rate <= 0:
            raise ValueError("learning_rate must be positive.")
        if self.weight_decay < 0:
            raise ValueError("weight_decay must not be negative.")

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class WindowPlan:
    """Logical dataset-window indices consumed by one experiment.

    Measurement indices cover warm-up and timed reads, train indices cover
    fixed optimizer steps, and validation indices cover the final loss. These
    are map-style dataset indices, not Paimon row IDs. ``sha256`` identifies
    the exact plan across independent backend processes.
    """

    seed: int
    measurement_indices: tuple
    train_indices: tuple
    validation_indices: tuple

    @property
    def sha256(self):
        payload = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self):
        return {
            "seed": self.seed,
            "measurement_indices": list(self.measurement_indices),
            "train_indices": list(self.train_indices),
            "validation_indices": list(self.validation_indices),
        }


def build_window_plan(train_window_count, validation_window_count, config):
    """Build deterministic measurement, training, and validation indices.

    Args:
        train_window_count: Number of complete windows in the train dataset.
        validation_window_count: Number of complete validation windows.
        config: Shared benchmark configuration supplying counts and the seed.

    Returns:
        A :class:`WindowPlan`. When more samples are needed than a dataset
        contains, consecutive seeded permutations are concatenated; sampling
        does not become independent sampling with replacement.
    """
    train_window_count = _positive_int(
        train_window_count, "train_window_count")
    validation_window_count = _positive_int(
        validation_window_count, "validation_window_count")
    batch_fetch_count = (
        config.warmup_batches + config.timed_batches) * config.batch_size
    train_count = config.optimizer_steps * config.batch_size
    return WindowPlan(
        seed=config.seed,
        measurement_indices=tuple(_repeat_permutations(
            train_window_count, batch_fetch_count, config.seed + 1)),
        train_indices=tuple(_repeat_permutations(
            train_window_count, train_count, config.seed + 2)),
        validation_indices=tuple(_repeat_permutations(
            validation_window_count, config.batch_size, config.seed + 3)),
    )


def decode_rgb_image(payload):
    """Decode JPEG/PNG bytes into an ``H x W x 3`` RGB NumPy array.

    Raises:
        ValueError: If Pillow cannot decode the payload as an image.
    """
    try:
        return np.asarray(Image.open(BytesIO(payload)).convert("RGB"))
    except Exception as error:
        raise ValueError("Cannot decode ACT RGB image bytes.") from error


def decode_image_tensor(value):
    """Decode bytes or an HDF5 uint8 value into normalized ``C x H x W``.

    The returned NumPy array is float32 with values in ``[0, 1]``. Both
    storage backends call this function so image conversion is not part of the
    performance difference being measured.
    """
    payload = (
        bytes(value)
        if isinstance(value, (bytes, bytearray, memoryview))
        else np.asarray(value, dtype=np.uint8).tobytes()
    )
    image = decode_rgb_image(payload)
    return np.transpose(image, (2, 0, 1)).astype(np.float32) / 255.0


def validate_act_batch(batch, config):
    """Validate a collated batch against the shared ACT tensor contract.

    Successful validation returns ``None``. It checks exact fields, tensor
    shapes and dtypes, finite values, image range, complete unpadded windows,
    and ``sample_id == episode_id#step_idx`` identity.
    """
    required = {
        "sample_id", "episode_id", "step_idx", "qpos", "action",
        "images", "is_pad",
    }
    if set(batch) != required:
        raise ValueError(
            "ACT batch fields differ: expected %s, got %s."
            % (sorted(required), sorted(batch)))
    batch_size = len(batch["sample_id"])
    expected = {
        "qpos": ((batch_size, 14), torch.float32),
        "action": ((batch_size, config.action_horizon, 14), torch.float32),
        "images": (
            (batch_size, len(CAMERA_KEYS), 3)
            + tuple(batch["images"].shape[-2:]),
            torch.float32,
        ),
        "is_pad": ((batch_size, config.action_horizon), torch.bool),
        "step_idx": ((batch_size,), torch.int64),
    }
    for name, (shape, dtype) in expected.items():
        value = batch[name]
        if not isinstance(value, torch.Tensor):
            raise ValueError("%s must be a torch.Tensor." % name)
        if tuple(value.shape) != shape:
            raise ValueError(
                "%s has shape %s; expected %s."
                % (name, tuple(value.shape), shape))
        if value.dtype != dtype:
            raise ValueError(
                "%s has dtype %s; expected %s." % (name, value.dtype, dtype))
    for name in ("qpos", "action", "images"):
        if not torch.isfinite(batch[name]).all():
            raise ValueError("%s contains NaN or Inf." % name)
    if torch.any(batch["images"] < 0) or torch.any(batch["images"] > 1):
        raise ValueError("images must be normalized to [0, 1].")
    if batch["is_pad"].any():
        raise ValueError("ACT benchmark windows must be complete and unpadded.")
    for sample_id, episode_id, step_idx in zip(
            batch["sample_id"], batch["episode_id"],
            batch["step_idx"].tolist()):
        if sample_id != "%s#%s" % (episode_id, step_idx):
            raise ValueError(
                "sample_id is not aligned with episode_id and step_idx.")


def build_lerobot_batch(batch, config):
    """Map a shared ACT batch to LeRobot ``ACTPolicy`` feature names.

    Images are resized bilinearly to the configured height and width when
    necessary. State, action, and padding retain their original semantics.
    """
    validate_act_batch(batch, config)
    images = batch["images"]
    target_size = (config.image_height, config.image_width)
    if tuple(images.shape[-2:]) != target_size:
        flat = images.flatten(0, 1)
        flat = functional.interpolate(
            flat, size=target_size, mode="bilinear", align_corners=False)
        images = flat.reshape(images.shape[:3] + target_size)
    result = {
        "observation.state": batch["qpos"],
        "action": batch["action"],
        "action_is_pad": batch["is_pad"],
    }
    for index, name in enumerate(CAMERA_KEYS):
        result[name] = images[:, index]
    return result


def build_act_policy(config):
    """Build the reduced CPU ACT policy used only by this benchmark.

    Returns:
        ``(policy, metadata)`` containing the LeRobot policy and a
        JSON-compatible description of its architecture and parameter counts.
        Pretrained weights are disabled, so this function performs no model
        download and does not represent a production training configuration.
    """
    try:
        import importlib.metadata
        from lerobot.configs.types import FeatureType, PolicyFeature
        from lerobot.policies.act.configuration_act import ACTConfig
        from lerobot.policies.act.modeling_act import ACTPolicy
    except ImportError as error:
        raise ImportError(
            "ACT benchmark requires: "
            "pip install -e '.[act]'.") from error

    inputs = {
        "observation.state": PolicyFeature(FeatureType.STATE, (14,)),
    }
    inputs.update({
        name: PolicyFeature(
            FeatureType.VISUAL,
            (3, config.image_height, config.image_width),
        )
        for name in CAMERA_KEYS
    })
    act_config = ACTConfig(
        input_features=inputs,
        output_features={
            "action": PolicyFeature(FeatureType.ACTION, (14,)),
        },
        device="cpu",
        chunk_size=config.action_horizon,
        n_action_steps=config.action_horizon,
        vision_backbone="resnet18",
        pretrained_backbone_weights=None,
        dim_model=64,
        n_heads=4,
        dim_feedforward=256,
        n_encoder_layers=1,
        n_decoder_layers=1,
        use_vae=True,
        latent_dim=16,
        n_vae_encoder_layers=1,
        kl_weight=10.0,
    )
    policy = ACTPolicy(act_config)
    return policy, {
        "implementation": "lerobot.ACTPolicy",
        "lerobot_version": importlib.metadata.version("lerobot"),
        "vision_backbone": act_config.vision_backbone,
        "pretrained_backbone_weights": act_config.pretrained_backbone_weights,
        "chunk_size": act_config.chunk_size,
        "dim_model": act_config.dim_model,
        "n_heads": act_config.n_heads,
        "n_encoder_layers": act_config.n_encoder_layers,
        "n_decoder_layers": act_config.n_decoder_layers,
        "n_vae_encoder_layers": act_config.n_vae_encoder_layers,
        "latent_dim": act_config.latent_dim,
        "kl_weight": act_config.kl_weight,
        "parameter_count": sum(
            parameter.numel() for parameter in policy.parameters()),
        "trainable_parameter_count": sum(
            parameter.numel()
            for parameter in policy.parameters() if parameter.requires_grad),
    }


def run_backend(
        backend,
        round_number,
        dataset_factory,
        plan,
        config,
        sample_sequence_sha256,
        policy_factory=None):
    """Measure one backend with the shared plan, model, and trainer.

    ``backend`` is a result label and ``round_number`` identifies the repeat.
    ``dataset_factory`` must return ``(train_dataset, validation_dataset)`` and
    must be reusable: it is called for the timed run and again by the separate
    Python-memory replay. ``policy_factory`` is an optional test hook returning
    ``(policy, model_metadata)``.

    Returns:
        A JSON-compatible metrics dictionary covering dataset construction,
        first batch, timed batch fetch, fixed optimizer steps, validation loss,
        and a separate ``tracemalloc`` peak replay. OS page cache is not
        controlled and native Arrow/Torch allocations are outside tracemalloc.
    """
    _seed_everything(config.seed)
    policy_factory = policy_factory or build_act_policy
    started = time.monotonic()
    dataset_started = time.monotonic()
    train_dataset, validation_dataset = dataset_factory()
    dataset_build_s = time.monotonic() - dataset_started

    warmup_sample_count = config.warmup_batches * config.batch_size
    warmup_iterator = _iter_logical_batches(
        train_dataset,
        plan.measurement_indices[:warmup_sample_count],
        logical_batch_size=config.batch_size,
        fetch_batches=1,
    )
    first_batch_started = time.monotonic()
    first_batch = next(warmup_iterator)
    first_batch_s = time.monotonic() - first_batch_started
    validate_act_batch(first_batch, config)
    for _ in range(config.warmup_batches - 1):
        validate_act_batch(next(warmup_iterator), config)

    batch_fetch_iterator = _iter_logical_batches(
        train_dataset,
        plan.measurement_indices[warmup_sample_count:],
        logical_batch_size=config.batch_size,
        fetch_batches=config.fetch_batches,
    )
    batch_fetch_seconds = 0.0
    batch_fetch_sample_count = 0
    for _ in range(config.timed_batches):
        batch_fetch_started = time.monotonic()
        batch = next(batch_fetch_iterator)
        batch_fetch_seconds += time.monotonic() - batch_fetch_started
        validate_act_batch(batch, config)
        batch_fetch_sample_count += len(batch["sample_id"])

    _seed_everything(config.seed)
    policy, model = policy_factory(config)
    parameters = (
        policy.get_optim_params()
        if hasattr(policy, "get_optim_params") else policy.parameters())
    optimizer = torch.optim.AdamW(
        parameters,
        lr=config.learning_rate,
        weight_decay=config.weight_decay,
    )
    policy.train()
    train_started = time.monotonic()
    losses = []
    for step, batch in enumerate(_iter_logical_batches(
            train_dataset,
            plan.train_indices,
            logical_batch_size=config.batch_size,
            fetch_batches=config.fetch_batches,
    ), 1):
        step_started = time.monotonic()
        model_batch = build_lerobot_batch(batch, config)
        optimizer.zero_grad(set_to_none=True)
        loss, components = policy(model_batch)
        if loss.ndim != 0 or not torch.isfinite(loss):
            raise FloatingPointError(
                "ACT produced a non-finite scalar loss at step %d." % step)
        loss.backward()
        optimizer.step()
        losses.append({
            "step": step,
            "total": float(loss.detach()),
            "components": {
                name: _finite_float(value, name)
                for name, value in components.items()
            },
            "step_time_s": time.monotonic() - step_started,
        })
    fixed_steps_s = time.monotonic() - train_started
    if len(losses) != config.optimizer_steps:
        raise AssertionError(
            "Expected %d optimizer steps, got %d."
            % (config.optimizer_steps, len(losses)))

    # ACTPolicy only constructs the VAE posterior needed by its supervised
    # loss while the module is in training mode. Keep that mode for validation
    # but disable gradients and parameter updates below.
    policy.train()
    _seed_everything(config.seed + 4)
    validation_batch = next(_iter_logical_batches(
        validation_dataset,
        plan.validation_indices,
        logical_batch_size=config.batch_size,
        fetch_batches=config.fetch_batches,
    ))
    with torch.no_grad():
        validation_loss, _ = policy(build_lerobot_batch(
            validation_batch, config))
    validation_value = _finite_float(validation_loss, "validation_loss")
    wall_time_s = time.monotonic() - started
    python_peak = _measure_python_peak(dataset_factory, plan, config)

    return {
        "round": round_number,
        "backend": backend,
        "sample_sequence_sha256": sample_sequence_sha256,
        "model": model,
        "optimizer": {
            "name": "AdamW",
            "learning_rate": config.learning_rate,
            "weight_decay": config.weight_decay,
        },
        "warmup_batches": config.warmup_batches,
        "first_batch_s": first_batch_s,
        "dataset_build_s": dataset_build_s,
        "batch_fetch_samples": batch_fetch_sample_count,
        "batch_fetch_s": batch_fetch_seconds,
        "batch_fetch_samples_per_s": (
            batch_fetch_sample_count / batch_fetch_seconds),
        "fixed_steps_s": fixed_steps_s,
        "train_loss": [item["total"] for item in losses],
        "train_trace": losses,
        "validation_loss": validation_value,
        "python_peak_allocated_bytes": python_peak,
        "peak_memory_measurement": (
            "python-tracemalloc-separate-dataset-first-batch"),
        "wall_time_s": wall_time_s,
    }


def _measure_python_peak(dataset_factory, plan, config):
    """Measure Python allocation peak in a separate dataset-first-batch replay.

    The factory is called again so tracing overhead cannot distort the main
    throughput timings. The returned integer is the tracemalloc peak in bytes.
    """
    gc.collect()
    tracemalloc.start()
    try:
        train_dataset, _ = dataset_factory()
        indices = plan.measurement_indices[
            :config.batch_size * config.fetch_batches
        ]
        next(_iter_logical_batches(
            train_dataset,
            indices,
            logical_batch_size=config.batch_size,
            fetch_batches=config.fetch_batches,
        ))
        _, peak = tracemalloc.get_traced_memory()
        return peak
    finally:
        tracemalloc.stop()


def _repeat_permutations(size, count, seed):
    """Return ``count`` indices by concatenating seeded permutations."""
    values = []
    generator = np.random.RandomState(seed)
    while len(values) < count:
        values.extend(generator.permutation(size).tolist())
    return values[:count]


def _seed_everything(seed):
    """Reset Python, NumPy, and Torch RNGs and enable deterministic Torch ops."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True)


def _finite_float(value, name):
    if isinstance(value, torch.Tensor):
        if value.numel() != 1:
            raise ValueError("%s must be scalar." % name)
        value = float(value.detach())
    else:
        value = float(value)
    if not math.isfinite(value):
        raise FloatingPointError("%s is NaN or Inf." % name)
    return value


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("%s must be a positive int." % name)
    return value


def _iter_logical_batches(
        dataset, indices, *, logical_batch_size, fetch_batches):
    """Yield collated model batches while coalescing physical dataset reads.

    Args:
        dataset: Map-style dataset implementing ``__getitem__`` and optionally
            plural ``__getitems__(indices)`` access.
        indices: Explicit ordered logical-window indices. Their count must be
            divisible by ``logical_batch_size``.
        logical_batch_size: Number of samples consumed by one model step.
        fetch_batches: Logical batches combined into one physical dataset read.

    Yields:
        Collated logical batches in the exact input-index order. A plural
        dataset method is preferred when available; otherwise samples are read
        individually and split back into the same logical batches.
    """
    logical_batch_size = _positive_int(
        logical_batch_size, "logical_batch_size")
    fetch_batches = _positive_int(fetch_batches, "fetch_batches")
    if len(indices) % logical_batch_size:
        raise ValueError("indices must contain complete logical batches.")
    physical_size = logical_batch_size * fetch_batches
    getitems = getattr(dataset, "__getitems__", None)
    for offset in range(0, len(indices), physical_size):
        physical_indices = list(indices[offset:offset + physical_size])
        if getitems is None:
            samples = [dataset[index] for index in physical_indices]
        else:
            samples = getitems(physical_indices)
        for logical_offset in range(0, len(samples), logical_batch_size):
            yield default_collate(
                samples[logical_offset:logical_offset + logical_batch_size])
