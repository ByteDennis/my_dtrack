from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from .common.hashing import HASH_BUCKET_SPACE, composite_key, hash32, key_families


def sample_csv(
    path: str | Path,
    *,
    key_columns: list[str],
    mode: str,
    rate: float | None = None,
    count: int | None = None,
    families: list[object] | None = None,
    key_tuples: list[list[object]] | None = None,
    side: str = "left",
) -> Path:
    if mode == "all":
        return Path(path)
    if not key_columns:
        raise ValueError("sampling requires key columns")
    frame = pd.read_csv(path, dtype=object)
    resolved = key_families(families, len(key_columns))
    composite = pd.Series(
        [
            composite_key(list(row), resolved, side)
            for row in frame[key_columns].itertuples(False)
        ],
        index=frame.index,
        dtype=object,
    )
    if mode == "fraction":
        threshold = max(1, round(float(rate) * HASH_BUCKET_SPACE))
        sampled = frame[composite.map(hash32) < threshold]
    elif mode == "fixed":
        if key_tuples is not None:
            wanted = {composite_key(list(row), resolved, "left") for row in key_tuples}
            sampled = frame[composite.isin(wanted)]
        else:
            ranked = frame.assign(
                __rank=composite.map(lambda text: hashlib.md5(text.encode()).hexdigest())
            )
            keys = ranked.sort_values("__rank")[key_columns].drop_duplicates().head(int(count))
            sampled = frame.merge(keys, on=key_columns, how="inner")
    else:
        raise ValueError(f"unsupported sample mode: {mode}")
    output = Path(path).with_name(Path(path).stem + ".sampled.csv")
    sampled.to_csv(output, index=False)
    return output
