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
