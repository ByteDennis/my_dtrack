"""Reusable interactive prompt functions for CLI workflows."""


def prompt_choice(message, choices, default=None):
    """Prompt user to pick from choices. Returns selected key.

    Args:
        message: Prompt message
        choices: Dict of {key: description}
        default: Default key if user presses Enter
    """
    choice_str = "/".join(
        f"[{k}]{v[1:]}" if len(v) > 1 else f"[{k}]"
        for k, v in choices.items()
    )
    if default:
        prompt = f"{message} {choice_str} (default: {default}): "
    else:
        prompt = f"{message} {choice_str}: "

    while True:
        try:
            resp = input(prompt).strip().lower()
        except EOFError:
            return default or list(choices.keys())[0]
        if not resp and default:
            return default
        if resp in choices:
            return resp
        # Try matching first char
        for k in choices:
            if resp == k[0]:
                return k
        print(f"  Invalid choice. Options: {', '.join(choices.keys())}")


def prompt_skip_pair(pair_name, summary_text):
    """Ask user whether to skip, continue, or ignore specific items.

    Returns: 'continue' | 'skip' | 'ignore'
    """
    print(f"\n{pair_name}: {summary_text}")
    return prompt_choice(
        "Action?",
        {"s": "skip pair", "i": "ignore dates", "c": "continue"},
        default="c"
    )


def prompt_ignore_items(items, item_type="rows"):
    """Show items, let user select which to ignore. Returns selected list."""
    if not items:
        return []

    print(f"\n  {item_type} ({len(items)}):")
    for i, item in enumerate(items, 1):
        print(f"    {i}. {item}")

    print(f"\n  Enter numbers to ignore (comma-separated), 'all', or empty to skip:")
    try:
        resp = input("  > ").strip()
    except EOFError:
        return []

    if not resp:
        return []
    if resp.lower() == 'all':
        return list(items)

    selected = []
    for part in resp.split(','):
        part = part.strip()
        try:
            idx = int(part) - 1
            if 0 <= idx < len(items):
                selected.append(items[idx])
        except ValueError:
            pass
    return selected


def prompt_mapping(left_items, right_items, existing_map=None):
    """Interactive mapping loop with wildcard support. Returns final map dict.

    Args:
        left_items: List of (name, type) tuples
        right_items: List of (name, type) tuples
        existing_map: Optional existing mappings dict
    """
    matched = dict(existing_map or {})
    left_names = {name for name, _ in left_items if name not in matched}
    right_names = {name for name, _ in right_items if name not in matched.values()}

    while left_names and right_names:
        print("Type column mappings (LEFT_COL  RIGHT_COL), wildcards supported (PREFIX_*  prefix_*)")
        print("Two empty lines to commit batch. Enter nothing twice to skip.")
        empty_count = 0
        batch = []

        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                empty_count = 2
                break
            if not line:
                empty_count += 1
                if empty_count >= 2:
                    break
            else:
                empty_count = 0
                batch.append(line)

        if not batch:
            break

        new_maps = _resolve_mapping_batch(batch, left_names, right_names)

        if new_maps:
            matched.update(new_maps)
            left_names -= set(new_maps.keys())
            right_names -= set(new_maps.values())
            print(f"  Mapped {len(new_maps)} columns")

        if not left_names or not right_names:
            print("All columns mapped!")
            break

        resp = prompt_choice(
            "Next?",
            {"c": "continue annotating", "f": "finish"},
            default="f"
        )
        if resp != "c":
            break

    return matched


def _resolve_mapping_batch(batch, left_names, right_names):
    """Process a batch of mapping entries (direct and wildcard)."""
    new_maps = {}
    for entry in batch:
        parts = entry.split()
        if len(parts) != 2:
            print(f"  Skipping invalid: {entry}")
            continue
        lp, rp = parts

        if '*' in lp or '*' in rp:
            l_prefix, l_suffix = lp.split('*', 1) if '*' in lp else (lp, '')
            r_prefix, r_suffix = rp.split('*', 1) if '*' in rp else (rp, '')

            for ln in list(left_names):
                if ln.startswith(l_prefix) and ln.endswith(l_suffix):
                    variable = ln[len(l_prefix):len(ln) - len(l_suffix) if l_suffix else len(ln)]
                    candidate = r_prefix + variable + r_suffix
                    for rn in list(right_names):
                        if rn.lower() == candidate.lower():
                            new_maps[ln] = rn
                            break
        else:
            actual_left = next((ln for ln in left_names if ln.lower() == lp.lower()), None)
            actual_right = next((rn for rn in right_names if rn.lower() == rp.lower()), None)
            if actual_left and actual_right:
                new_maps[actual_left] = actual_right
            else:
                missing = []
                if not actual_left:
                    missing.append(f"left: {lp}")
                if not actual_right:
                    missing.append(f"right: {rp}")
                print(f"  Not found: {', '.join(missing)}")

    return new_maps


def prompt_col_type_override(col_name, left_type, right_type):
    """Ask user to resolve mismatched column types."""
    print(f"\n  Column {col_name}: type mismatch (left: {left_type}, right: {right_type})")
    return prompt_choice(
        "  Use",
        {"c": "categorical", "n": "numeric", "s": "skip column"},
        default="c"
    )


def save_and_pause(config, config_path, edit_hints=None):
    """Save config, show edit hints, pause for manual editing, reload.

    Returns reloaded config dict.
    """
    import json
    from .config import save_unified_config

    save_unified_config(config, config_path)

    print(f"\n{'='*80}")
    print("Review and edit the config file now")
    print(f"{'='*80}")
    print(f"  File: {config_path}")
    if edit_hints:
        print()
        for hint in edit_hints:
            print(f"  - {hint}")
    print()
    input("Press Enter when done editing...")

    print(f"\nReloading config from {config_path}")
    with open(config_path, 'r') as f:
        return json.load(f)


def confirm(prompt_text="Proceed? [y/N] "):
    """Ask user for confirmation. Returns True if confirmed."""
    try:
        resp = input(prompt_text).strip().lower()
    except EOFError:
        return False
    return resp in ('y', 'yes')
