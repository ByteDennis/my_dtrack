from __future__ import annotations

from pathlib import Path

import pandas as pd

from .normalization import HASH_BUCKET_SPACE, normalize_value, selected_fraction


def sample_csv(
    path: str | Path,
    *,
    key_columns: list[str],
    mode: str,
    rate: float | None = None,
    count: int | None = None,
    families: list[str] | None = None,
    key_tuples: list[list[object]] | None = None,
) -> Path:
    if mode == "all":
        return Path(path)
    if not key_columns:
        raise ValueError("sampling requires key columns")
    frame = pd.read_csv(path, dtype=object)
    families = families or ["string"] * len(key_columns)
    if mode == "fraction":
        threshold = max(1, round(float(rate) * HASH_BUCKET_SPACE))
        mask = frame[key_columns].apply(
            lambda row: selected_fraction(list(row), families, threshold), axis=1
        )
        sampled = frame[mask]
    elif mode == "fixed":
        if key_tuples is not None:
            wanted = {
                tuple(normalize_value(value, family) for value, family in zip(row, families))
                for row in key_tuples
            }
            mask = frame[key_columns].apply(
                lambda row: (
                    tuple(normalize_value(value, family) for value, family in zip(row, families))
                    in wanted
                ),
                axis=1,
            )
            sampled = frame[mask]
        else:
            ranked = frame.assign(
                __rank=frame[key_columns].apply(
                    lambda row: (
                        __import__("hashlib")
                        .md5(
                            "|".join(
                                normalize_value(value, family)
                                for value, family in zip(row, families)
                            ).encode()
                        )
                        .hexdigest()
                    ),
                    axis=1,
                )
            )
            keys = ranked.sort_values("__rank")[key_columns].drop_duplicates().head(int(count))
            sampled = frame.merge(keys, on=key_columns, how="inner")
    else:
        raise ValueError(f"unsupported sample mode: {mode}")
    output = Path(path).with_name(Path(path).stem + ".sampled.csv")
    sampled.to_csv(output, index=False)
    return output
