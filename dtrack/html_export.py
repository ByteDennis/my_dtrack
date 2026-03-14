"""HTML export functionality for comparison reports"""

from typing import Dict, List, Optional
from datetime import datetime


def _comment_icon(comment):
    """Return ⓘ tooltip HTML if comment is non-empty, else empty string."""
    if not comment:
        return ''
    # Escape HTML entities in comment
    safe = comment.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
    return f' <span title="{safe}" style="cursor:help; color:#5b7083;">ⓘ</span>'


def generate_row_count_html(
    pair_name: str,
    source_left: str,
    source_right: str,
    table_left: str,
    table_right: str,
    comparison: Dict,
    metadata_left: Optional[Dict] = None,
    metadata_right: Optional[Dict] = None,
    where_map: Optional[Dict[str, str]] = None,
    time_map: Optional[Dict[str, str]] = None,
    comment_left: str = '',
    comment_right: str = '',
    left_cfg: Optional[Dict] = None,
    right_cfg: Optional[Dict] = None,
) -> str:
    """
    Generate HTML table rows for row count comparison.

    Args:
        pair_name: Name of the table pair
        source_left: Left source label (e.g., "pcds", "oracle")
        source_right: Right source label (e.g., "aws")
        table_left: Left table name
        table_right: Right table name
        comparison: Result from compare_row_counts()
        metadata_left: Optional metadata for left table
        metadata_right: Optional metadata for right table
        comment_left: Optional comment for left source (shown as ⓘ tooltip)
        comment_right: Optional comment for right source (shown as ⓘ tooltip)

    Returns:
        HTML string with table rows
    """
    summary = comparison['summary']

    # Count statistics
    n_match = len(comparison['matching'])
    n_mismatch = len(comparison['mismatched'])
    n_only_left = len(comparison['only_left'])
    n_only_right = len(comparison['only_right'])

    # Build banner text
    banner_text = pair_name
    if left_cfg and right_cfg:
        tbl_l = left_cfg.get('table', table_left).upper()
        tbl_r = right_cfg.get('table', table_right).upper()
        conn_l = left_cfg.get('conn_macro', '').upper()
        conn_r = right_cfg.get('conn_macro', '').upper()
        banner_text = f'{pair_name} | <b>{tbl_l}</b>{f" ({conn_l})" if conn_l else ""} | <b>{tbl_r}</b>{f" ({conn_r})" if conn_r else ""}'

    # Generate HTML rows
    html = f'''
            <!-- {pair_name} -->
            <tr>
                <td colspan="6" style="border:1px solid #ccc; padding:8px; background:#e8f0fe; font-weight:600;">
                    {banner_text}
                </td>
            </tr>
'''

    # Get date columns and format row counts (with vintage suffix)
    date_col_left = (metadata_left.get('date_var') or '—') if metadata_left else '—'
    date_col_right = (metadata_right.get('date_var') or '—') if metadata_right else '—'
    vintage_left = metadata_left.get('vintage', '') if metadata_left else ''
    vintage_right = metadata_right.get('vintage', '') if metadata_right else ''
    if vintage_left and date_col_left != '—':
        date_col_left = f"{date_col_left} ({vintage_left.upper()})"
    if vintage_right and date_col_right != '—':
        date_col_right = f"{date_col_right} ({vintage_right.upper()})"

    # Format row counts with billions
    def format_count(count):
        if count >= 1_000_000_000:
            billions = count / 1_000_000_000
            return f'{count:,} ({billions:.2f} B)'
        else:
            millions = count / 1_000_000
            return f'{count:,} ({millions:.2f} M)'

    # Data rows
    time_left = '—'
    time_right = '—'
    def _fmt_time(val):
        """Format time value: raw seconds (int/float) → human readable, strings pass through."""
        if val is None or val == '—' or val == '':
            return '—'
        try:
            secs = float(val)
        except (TypeError, ValueError):
            return str(val)  # already a string like "42 sec"
        if secs < 60:
            return f"{secs:.0f} sec"
        elif secs < 3600:
            m, s = divmod(int(secs), 60)
            return f"{m} min {s} sec" if s else f"{m} min"
        else:
            h, rem = divmod(int(secs), 3600)
            m = rem // 60
            return f"{h} hour {m} min" if m else f"{h} hour"

    if time_map:
        time_left = _fmt_time(time_map.get(source_left, time_map.get('left', '—')))
        time_right = _fmt_time(time_map.get(source_right, time_map.get('right', '—')))

    # Calculate overlap for highlighting
    left_min, left_max = summary['date_range_left']
    right_min, right_max = summary['date_range_right']
    has_overlap = left_min and right_min and left_max and right_max
    overlap_start = overlap_end = None
    if has_overlap:
        overlap_start = max(left_min, right_min)
        overlap_end = min(left_max, right_max)
        if overlap_start > overlap_end:
            has_overlap = False

    # Style helpers for min/max overlap highlighting
    def _min_style(val):
        base = 'border:1px solid #ccc; padding:8px;'
        if has_overlap and val and val == overlap_start:
            return f'{base} color:#1565c0; font-weight:600;'
        return base

    def _max_style(val, other_val):
        base = 'border:1px solid #ccc; padding:8px;'
        parts = []
        if has_overlap and val and val == overlap_end:
            parts.append('color:#1565c0; font-weight:600;')
        if val and other_val and val != other_val:
            parts.append('text-decoration:underline;')
        if parts:
            return f'{base} {" ".join(parts)}'
        return base

    html += f'''            <tr>
                <td style="border:1px solid #ccc; padding:8px;">{source_left.upper()}{_comment_icon(comment_left)}</td>
                <td style="border:1px solid #ccc; padding:8px;">{date_col_left}</td>
                <td style="{_min_style(left_min)}">{left_min or '—'}</td>
                <td style="{_max_style(left_max, right_max)}">{left_max or '—'}</td>
                <td style="border:1px solid #ccc; padding:8px;">{format_count(summary['total_left'])}</td>
                <td style="border:1px solid #ccc; padding:8px;">{time_left}</td>
            </tr>
            <tr>
                <td style="border:1px solid #ccc; padding:8px;">{source_right.upper()}{_comment_icon(comment_right)}</td>
                <td style="border:1px solid #ccc; padding:8px;">{date_col_right}</td>
                <td style="{_min_style(right_min)}">{right_min or '—'}</td>
                <td style="{_max_style(right_max, left_max)}">{right_max or '—'}</td>
                <td style="border:1px solid #ccc; padding:8px;">{format_count(summary['total_right'])}</td>
                <td style="border:1px solid #ccc; padding:8px;">{time_right}</td>
            </tr>
'''

    all_only_left = comparison['only_left']
    all_only_right = comparison['only_right']

    # Split only-left/only-right into overlap vs outside
    if has_overlap:
        only_left_in_overlap = [(dt, c) for dt, c in all_only_left if overlap_start <= dt <= overlap_end]
        only_left_outside = [(dt, c) for dt, c in all_only_left if not (overlap_start <= dt <= overlap_end)]
        only_right_in_overlap = [(dt, c) for dt, c in all_only_right if overlap_start <= dt <= overlap_end]
        only_right_outside = [(dt, c) for dt, c in all_only_right if not (overlap_start <= dt <= overlap_end)]
    else:
        only_left_in_overlap = []
        only_left_outside = all_only_left
        only_right_in_overlap = []
        only_right_outside = all_only_right

    # Details row with summary stats as clickable header
    # Red only when there are misloads (one-sided dates inside overlap range)
    only_left_style = 'color:#c62828; font-weight:600;' if len(only_left_in_overlap) > 0 else ''
    only_right_style = 'color:#c62828; font-weight:600;' if len(only_right_in_overlap) > 0 else ''
    mismatch_style = 'color:#c62828; font-weight:600;' if n_mismatch > 0 else ''
    summary_text = f'<span style="{only_left_style}">{source_left} only: {n_only_left}</span>, <span style="{only_right_style}">{source_right} only: {n_only_right}</span>, <span style="color:green;">days match: {n_match}</span>, <span style="{mismatch_style}">days mismatch: {n_mismatch}</span>'

    html += f'''            <tr>
                <td colspan="6" style="border:1px solid #ccc; padding:4px;">
                    <details style="margin:0;">
                        <summary style="cursor:pointer; padding:4px; list-style:none; font-weight:500; font-size:12px;">
                            {summary_text}
                        </summary>
                        <div style="padding:8px; font-size:12px;">
'''

    # If no issues, show message
    if n_mismatch == 0 and n_only_left == 0 and n_only_right == 0:
        html += '                            <p style="margin:8px 0; color:green;">✓ All dates match perfectly!</p>\n'
    else:
        # Side-by-side layout with 3 columns separated by bars
        bar_style = 'border-left:1px solid #ddd;'
        html += '                            <table style="width:100%; border:none; border-spacing:0;">\n'
        html += '                                <tr style="vertical-align:top;">\n'
        source_left, source_right = source_left.upper(), source_right.upper()

        # Column 1: Mismatch details
        html += '                                    <td style="width:32%; padding:0 12px 0 0; border:none;">\n'
        if n_mismatch > 0:
            html += f'                                        <p style="margin:4px 0; font-weight:600; text-align:center;">Mismatched Dates ({n_mismatch})</p>\n'
            html += '                                        <table style="border-collapse:collapse; font-size:11px; width:100%;">\n'
            html += f'                                            <tr><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Date</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_left}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_right}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Diff</th></tr>\n'
            for dt, count_left, count_right in comparison['mismatched'][:10]:
                diff = count_right - count_left
                sign = '+' if diff > 0 else ''
                html += f'                                            <tr><td style="padding:2px; font-size:10px;">{dt}</td><td style="padding:2px; font-size:10px;">{count_left:,}</td><td style="padding:2px; font-size:10px;">{count_right:,}</td><td style="padding:2px; font-size:10px;">{sign}{diff:,}</td></tr>\n'
            if n_mismatch > 10:
                html += f'                                            <tr><td colspan="4" style="padding:2px; font-style:italic; font-size:10px; color:#999;">... and {n_mismatch - 10} more</td></tr>\n'
            html += '                                        </table>\n'
        else:
            html += '                                        <p style="margin:4px 0; color:#999;">No mismatches</p>\n'
        html += '                                    </td>\n'

        # Column 2: Only-left details — overlap (red) then outside (gray)
        html += f'                                    <td style="width:32%; padding:0 12px; {bar_style}">\n'
        if n_only_left > 0:
            html += f'                                        <p style="margin:4px 0; font-weight:600; text-align:center;">{source_left}-Only Dates ({n_only_left})</p>\n'
            html += '                                        <table style="border-collapse:collapse; font-size:11px; width:100%;">\n'
            html += f'                                            <tr><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Date</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_left}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_right}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Diff</th></tr>\n'
            # In-overlap rows (date in red, rest black)
            for dt, count in only_left_in_overlap[:10]:
                html += f'                                            <tr><td style="padding:2px; font-size:10px; color:#c62828;">{dt}</td><td style="padding:2px; font-size:10px;">{count:,}</td><td style="padding:2px; font-size:10px;">0</td><td style="padding:2px; font-size:10px;">-{count:,}</td></tr>\n'
            if len(only_left_in_overlap) > 10:
                html += f'                                            <tr><td colspan="4" style="padding:2px; font-style:italic; font-size:10px; color:#c62828;">... and {len(only_left_in_overlap) - 10} more in overlap</td></tr>\n'
            # Outside-overlap: 1 gray row + summary
            if only_left_outside:
                dt0, c0 = only_left_outside[0]
                html += f'                                            <tr style="color:#999;"><td style="padding:2px; font-size:10px;">{dt0}</td><td style="padding:2px; font-size:10px;">{c0:,}</td><td style="padding:2px; font-size:10px;">0</td><td style="padding:2px; font-size:10px;">-{c0:,}</td></tr>\n'
                if len(only_left_outside) > 1:
                    html += f'                                            <tr><td colspan="4" style="padding:4px 2px 2px; font-style:italic; font-size:10px; color:#999;">... {len(only_left_outside) - 1} more outside overlap</td></tr>\n'
            html += '                                        </table>\n'
        else:
            html += f'                                        <p style="margin:4px 0; color:#999;">No {source_left}-Only dates</p>\n'
        html += '                                    </td>\n'

        # Column 3: Only-right details — overlap (red) then outside (gray)
        html += f'                                    <td style="width:32%; padding:0 0 0 12px; {bar_style}">\n'
        if n_only_right > 0:
            html += f'                                        <p style="margin:4px 0; font-weight:600; text-align:center;">{source_right}-Only Dates ({n_only_right})</p>\n'
            html += '                                        <table style="border-collapse:collapse; font-size:11px; width:100%;">\n'
            html += f'                                            <tr><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Date</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_left}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">{source_right}</th><th style="border-bottom:1px solid #ddd; padding:3px; text-align:left;">Diff</th></tr>\n'
            # In-overlap rows (date in red, rest black)
            for dt, count in only_right_in_overlap[:10]:
                html += f'                                            <tr><td style="padding:2px; font-size:10px; color:#c62828;">{dt}</td><td style="padding:2px; font-size:10px;">0</td><td style="padding:2px; font-size:10px;">{count:,}</td><td style="padding:2px; font-size:10px;">+{count:,}</td></tr>\n'
            if len(only_right_in_overlap) > 10:
                html += f'                                            <tr><td colspan="4" style="padding:2px; font-style:italic; font-size:10px; color:#c62828;">... and {len(only_right_in_overlap) - 10} more in overlap</td></tr>\n'
            # Outside-overlap: 1 gray row + summary
            if only_right_outside:
                dt0, c0 = only_right_outside[0]
                html += f'                                            <tr style="color:#999;"><td style="padding:2px; font-size:10px;">{dt0}</td><td style="padding:2px; font-size:10px;">0</td><td style="padding:2px; font-size:10px;">{c0:,}</td><td style="padding:2px; font-size:10px;">+{c0:,}</td></tr>\n'
                if len(only_right_outside) > 1:
                    html += f'                                            <tr><td colspan="4" style="padding:4px 2px 2px; font-style:italic; font-size:10px; color:#999;">... {len(only_right_outside) - 1} more outside overlap</td></tr>\n'
            html += '                                        </table>\n'
        else:
            html += f'                                        <p style="margin:4px 0; color:#999;">No {source_right}-Only dates</p>\n'
        html += '                                    </td>\n'

        html += '                                </tr>\n'
        html += '                            </table>\n'

    html += '''                        </div>
                    </details>
                </td>
            </tr>

'''

    return html


def generate_column_stats_html(
    pair_name: str,
    source_left: str,
    source_right: str,
    table_left: str,
    table_right: str,
    comparison: Dict[str, List[Dict]],
    col_mappings: Dict[str, str],
    metadata_left: Optional[Dict] = None,
    metadata_right: Optional[Dict] = None,
    time_map: Optional[Dict[str, str]] = None,
    comment_left: str = '',
    comment_right: str = '',
    left_cfg: Optional[Dict] = None,
    right_cfg: Optional[Dict] = None,
) -> str:
    """
    Generate HTML table rows for column statistics comparison.

    Args:
        pair_name: Name of the table pair
        source_left: Left source label
        source_right: Right source label
        table_left: Left table name
        table_right: Right table name
        comparison: Result from compare_column_stats()
        col_mappings: Column mappings used
        comment_left: Optional comment for left source (shown as ⓘ tooltip)
        comment_right: Optional comment for right source (shown as ⓘ tooltip)

    Returns:
        HTML string with table rows
    """
    # Organize data by vintage and column
    vintages_data = {}
    all_dates = set()

    for col, comparisons in comparison.items():
        for comp in comparisons:
            dt = comp['dt']
            all_dates.add(dt)
            if dt not in vintages_data:
                vintages_data[dt] = {}
            vintages_data[dt][col] = comp

    sorted_dates = sorted(all_dates, key=lambda d: d[:10] if len(d) > 10 else d, reverse=True)

    # Count columns with differences
    cols_with_diffs = set()
    for col, comparisons in comparison.items():
        for comp in comparisons:
            if _has_differences(comp):
                cols_with_diffs.add(col)

    n_cols = len(comparison)
    n_diff = len(cols_with_diffs)
    n_match = n_cols - n_diff

    # Format column list with truncation
    if len(cols_with_diffs) > 4:
        cols_display = ', '.join(sorted(list(cols_with_diffs)[:4])) + ', ...'
    else:
        cols_display = ', '.join(sorted(cols_with_diffs)) if cols_with_diffs else 'None'

    # Build banner text
    banner_text = pair_name
    if left_cfg and right_cfg:
        tbl_l = left_cfg.get('table', table_left).upper()
        tbl_r = right_cfg.get('table', table_right).upper()
        conn_l = left_cfg.get('conn_macro', '').upper()
        conn_r = right_cfg.get('conn_macro', '').upper()
        banner_text = f'{pair_name} | <b>{tbl_l}</b>{f" ({conn_l})" if conn_l else ""} | <b>{tbl_r}</b>{f" ({conn_r})" if conn_r else ""}'

    # Generate HTML rows
    html = f'''
            <!-- {pair_name} -->
            <tr>
                <td colspan="7" style="border:1px solid #ccc; padding:8px; background:#e8f0fe; font-weight:600;">
                    {banner_text}
                </td>
            </tr>
'''

    # Data rows - show overlap period (dates in common)
    min_date = sorted_dates[-1] if sorted_dates else '—'
    max_date = sorted_dates[0] if sorted_dates else '—'
    n_vintages = len(sorted_dates)

    # Get date columns from metadata (with vintage suffix)
    date_col_left = (metadata_left.get('date_var') or '—') if metadata_left else '—'
    date_col_right = (metadata_right.get('date_var') or '—') if metadata_right else '—'
    vintage_left = metadata_left.get('vintage', '') if metadata_left else ''
    vintage_right = metadata_right.get('vintage', '') if metadata_right else ''
    if vintage_left and date_col_left != '—':
        date_col_left = f"{date_col_left} ({vintage_left.upper()})"
    if vintage_right and date_col_right != '—':
        date_col_right = f"{date_col_right} ({vintage_right.upper()})"

    time_left = '—'
    time_right = '—'
    if time_map:
        time_left = _fmt_time(time_map.get(source_left, time_map.get('left', '—')))
        time_right = _fmt_time(time_map.get(source_right, time_map.get('right', '—')))

    # Blue overlap boundary styling for min/max dates
    overlap_style = 'border:1px solid #ccc; padding:8px; color:#1565c0; font-weight:600;'
    cell = 'border:1px solid #ccc; padding:8px;'

    html += f'''            <tr>
                <td style="{cell}">{source_left.upper()}{_comment_icon(comment_left)}</td>
                <td style="{cell}">{date_col_left}</td>
                <td style="{overlap_style}">{min_date}</td>
                <td style="{overlap_style}">{max_date}</td>
                <td style="{cell}">{n_cols}</td>
                <td style="{cell}">{n_vintages}</td>
                <td style="{cell}">{time_left}</td>
            </tr>
            <tr>
                <td style="{cell}">{source_right.upper()}{_comment_icon(comment_right)}</td>
                <td style="{cell}">{date_col_right}</td>
                <td style="{overlap_style}">{min_date}</td>
                <td style="{overlap_style}">{max_date}</td>
                <td style="{cell}">{n_cols}</td>
                <td style="{cell}">{n_vintages}</td>
                <td style="{cell}">{time_right}</td>
            </tr>
'''

    # Details row with summary stats
    diff_style = 'color:#c62828; font-weight:600;' if n_diff > 0 else 'color:green;'
    summary_text = f'{n_cols} columns: <span style="color:green;">{n_match} match</span>, <span style="{diff_style}">{n_diff} diff</span> ({cols_display})'

    html += f'''            <tr>
                <td colspan="7" style="border:1px solid #ccc; padding:4px;">
                    <details style="margin:0;">
                        <summary style="cursor:pointer; padding:4px; list-style:none; font-weight:500; font-size:12px;">
                            {summary_text}
                        </summary>
                        <div style="padding:8px; font-size:12px;">
'''

    # If no diffs, show message
    if n_diff == 0:
        html += '                            <p style="margin:8px 0; color:green;">✓ All columns match across all vintages!</p>\n'
    else:
        # Group diffs by column (compact format)
        col_diffs = {}  # {col: [(vintage, stat, left, right, diff), ...]}

        for dt in sorted_dates:
            vintage_data = vintages_data.get(dt, {})
            for col in sorted(vintage_data.keys()):
                comp = vintage_data[col]
                if not _has_differences(comp):
                    continue

                if col not in col_diffs:
                    col_diffs[col] = []

                # Collect differing stats (use vintage_label for display)
                vlabel = comp.get('vintage_label', dt)
                if comp.get('n_total_diff', 0) != 0:
                    col_diffs[col].append((vlabel, 'n_total', comp['n_total_left'], comp['n_total_right'], comp['n_total_diff']))
                if comp.get('n_missing_diff', 0) != 0:
                    col_diffs[col].append((vlabel, 'n_missing', comp['n_missing_left'], comp['n_missing_right'], comp['n_missing_diff']))
                if comp.get('n_unique_diff', 0) != 0:
                    col_diffs[col].append((vlabel, 'n_unique', comp['n_unique_left'], comp['n_unique_right'], comp['n_unique_diff']))
                if comp['col_type'] == 'numeric':
                    if comp.get('mean_diff') is not None and abs(comp.get('mean_diff', 0)) > 0.01:
                        col_diffs[col].append((vlabel, 'mean', comp.get('mean_left'), comp.get('mean_right'), comp.get('mean_diff')))
                    if comp.get('std_diff') is not None and abs(comp.get('std_diff', 0)) > 0.01:
                        col_diffs[col].append((vlabel, 'std', comp.get('std_left'), comp.get('std_right'), comp.get('std_diff')))

        # Prepare column details for 3-column layout
        col_details_list = []
        for col in sorted(col_diffs.keys()):
            diffs = col_diffs[col]

            # Get column type and display name
            first_comp = None
            for dt in sorted_dates:
                if col in vintages_data.get(dt, {}):
                    first_comp = vintages_data[dt][col]
                    break

            col_type = first_comp['col_type'] if first_comp else 'unknown'
            right_col = first_comp['right_col'] if first_comp else col
            col_display = f'{col} → {right_col}' if col != right_col else col

            # Build column detail HTML
            col_html = f'<p style="margin:4px 0 2px 0; font-weight:600; color:#1f2933; font-size:11px;">{col_display} ({col_type}) - {len(set(d[0] for d in diffs))} diffs:</p>\n'
            col_html += '<div style="margin-left:12px; font-size:10px; line-height:1.5;">\n'

            # Group by vintage
            vintage_diffs = {}
            for dt, stat, left, right, diff in diffs:
                if dt not in vintage_diffs:
                    vintage_diffs[dt] = []
                vintage_diffs[dt].append((stat, left, right, diff))

            for dt in sorted(vintage_diffs.keys(), reverse=True)[:3]:  # Show max 3 vintages per column
                stats_list = vintage_diffs[dt]
                stats_display = []
                for stat, left, right, diff in stats_list:
                    if isinstance(diff, float):
                        stats_display.append(f'{stat} {diff:+.2f} ({left:.2f}→{right:.2f})')
                    else:
                        stats_display.append(f'{stat} {diff:+,} ({left:,}→{right:,})')

                col_html += f'├─ {dt}: {", ".join(stats_display)}<br>\n'

            if len(vintage_diffs) > 3:
                col_html += f'└─ ... +{len(vintage_diffs) - 3} more<br>\n'

            col_html += '</div>\n'

            col_details_list.append(col_html)

        # Display in 3-column grid
        html += '                            <table style="width:100%; border:none;">\n'
        html += '                                <tr style="vertical-align:top;">\n'

        # Distribute columns into 3 groups
        num_cols = len(col_details_list)
        cols_per_group = (num_cols + 2) // 3  # Ceiling division

        for i in range(3):
            html += '                                    <td style="width:33%; padding-right:10px; border:none;">\n'

            start_idx = i * cols_per_group
            end_idx = min((i + 1) * cols_per_group, num_cols)

            for col_html in col_details_list[start_idx:end_idx]:
                html += col_html

            html += '                                    </td>\n'

        html += '                                </tr>\n'
        html += '                            </table>\n'

    html += '''                        </div>
                    </details>
                </td>
            </tr>

'''

    return html


def _has_differences(comp: Dict) -> bool:
    """Check if comparison has any differences"""
    # Check count differences
    if comp.get('n_total_diff', 0) != 0:
        return True
    if comp.get('n_missing_diff', 0) != 0:
        return True
    if comp.get('n_unique_diff', 0) != 0:
        return True

    # Check numeric differences
    if comp['col_type'] == 'numeric':
        if comp.get('mean_diff') is not None and abs(comp.get('mean_diff', 0)) > 0.01:
            return True
        if comp.get('std_diff') is not None and abs(comp.get('std_diff', 0)) > 0.01:
            return True

    return False


def _get_worst_stat(comp: Dict) -> str:
    """Get the stat with the worst difference"""
    if comp.get('n_total_diff', 0) != 0:
        pct = abs(comp['n_total_diff'] / comp['n_total_left'] * 100) if comp['n_total_left'] > 0 else 0
        return f'n_total {pct:.1f}%'
    if comp.get('n_missing_diff', 0) != 0:
        return 'n_missing'
    if comp['col_type'] == 'numeric' and comp.get('mean_diff') is not None:
        if abs(comp.get('mean_diff', 0)) > 0.01:
            pct = abs(comp['mean_diff'] / comp['mean_left'] * 100) if comp.get('mean_left', 0) != 0 else 0
            return f'mean {pct:.1f}%'
    if comp.get('n_unique_diff', 0) != 0:
        return 'n_unique'

    return 'other'


def _generate_numeric_detail_table(comp: Dict, source_left: str, source_right: str) -> str:
    """Generate detail table for numeric column"""
    stats = [
        ('n_total', comp['n_total_left'], comp['n_total_right'], comp['n_total_diff']),
        ('n_missing', comp['n_missing_left'], comp['n_missing_right'], comp['n_missing_diff']),
        ('n_unique', comp['n_unique_left'], comp['n_unique_right'], comp['n_unique_diff']),
        ('mean', comp.get('mean_left'), comp.get('mean_right'), comp.get('mean_diff')),
        ('std', comp.get('std_left'), comp.get('std_right'), comp.get('std_diff')),
        ('min', comp.get('min_left'), comp.get('min_right'), None),
        ('max', comp.get('max_left'), comp.get('max_right'), None),
    ]

    html = '                        <table class="stat-table">\n'
    html += '                            <thead>\n'
    html += f'                                <tr><th>stat</th><th>{source_left}</th><th>{source_right}</th><th>diff</th><th>% diff</th></tr>\n'
    html += '                            </thead>\n'
    html += '                            <tbody>\n'

    matching_stats = []

    for stat_name, left_val, right_val, diff in stats:
        if diff is not None and diff != 0:
            # Calculate % difference
            pct_diff = ''
            if left_val is not None and left_val != 0 and diff is not None:
                pct = (diff / left_val) * 100
                sign = '+' if pct > 0 else ''
                pct_diff = f'{sign}{pct:.1f}%'

            # Format diff
            if isinstance(diff, float):
                diff_str = f'{diff:+.2f}'
            else:
                diff_str = f'{diff:+,}'

            # Format values
            left_str = f'{left_val:,.2f}' if isinstance(left_val, float) else f'{left_val:,}'
            right_str = f'{right_val:,.2f}' if isinstance(right_val, float) else f'{right_val:,}'

            html += f'                                <tr><td>{stat_name}</td><td>{left_str}</td><td>{right_str}</td><td class="status-red">{diff_str}</td><td>{pct_diff}</td></tr>\n'
        elif left_val == right_val:
            matching_stats.append(stat_name)

    html += '                            </tbody>\n'
    html += '                        </table>\n'

    if matching_stats:
        html += f'                        <p class="matching-stats">({", ".join(matching_stats)}: match)</p>\n'

    return html


def _generate_categorical_detail_table(comp: Dict, source_left: str, source_right: str) -> str:
    """Generate detail table for categorical column"""
    import json

    html = '                        <table class="stat-table">\n'
    html += '                            <thead>\n'
    html += f'                                <tr><th>stat</th><th>{source_left}</th><th>{source_right}</th><th>diff</th></tr>\n'
    html += '                            </thead>\n'
    html += '                            <tbody>\n'

    stats = [
        ('n_total', comp['n_total_left'], comp['n_total_right'], comp['n_total_diff']),
        ('n_missing', comp['n_missing_left'], comp['n_missing_right'], comp['n_missing_diff']),
        ('n_unique', comp['n_unique_left'], comp['n_unique_right'], comp['n_unique_diff']),
    ]

    for stat_name, left_val, right_val, diff in stats:
        if diff != 0:
            html += f'                                <tr><td>{stat_name}</td><td>{left_val:,}</td><td>{right_val:,}</td><td class="status-red">{diff:+,}</td></tr>\n'

    html += '                            </tbody>\n'
    html += '                        </table>\n'

    # Show top_10 changes if different
    top10_left = comp.get('top_10_left')
    top10_right = comp.get('top_10_right')

    if top10_left and top10_right and top10_left != top10_right:
        try:
            left_dict = json.loads(top10_left) if isinstance(top10_left, str) else top10_left
            right_dict = json.loads(top10_right) if isinstance(top10_right, str) else top10_right

            # Find values that changed
            all_values = set(left_dict.keys()) | set(right_dict.keys())
            changes = []

            for val in all_values:
                left_count = left_dict.get(val, 0)
                right_count = right_dict.get(val, 0)
                if left_count != right_count:
                    diff = right_count - left_count
                    changes.append((val, left_count, right_count, diff))

            if changes:
                html += '                        <div class="top10-changes">\n'
                html += '                            <strong>top_10 changes:</strong><br>\n'
                for val, left_count, right_count, diff in changes[:5]:  # Show top 5 changes
                    sign = '+' if diff > 0 else ''
                    html += f'                            {val}: {source_left} {left_count:,} → {source_right} {right_count:,} ({sign}{diff:,})<br>\n'
                html += '                        </div>\n'
        except:
            pass

    return html


def create_row_count_table(row_sections: List[str]) -> str:
    """
    Wrap row count comparison rows in a single table.

    Args:
        row_sections: List of HTML row strings from generate_row_count_html()

    Returns:
        Complete HTML table string
    """
    html = '''
    <table style="border-collapse:collapse; width:100%; font-family:Segoe UI, Arial, sans-serif; font-size:12.5px; table-layout:fixed;">
        <colgroup>
            <col style="width:120px;"> <!-- Environment -->
            <col style="width:140px;"> <!-- Date Variable -->
            <col style="width:110px;"> <!-- Min Date -->
            <col style="width:110px;"> <!-- Max Date -->
            <col style="width:160px;"> <!-- Row Count -->
            <col style="width:120px;"> <!-- Query Runtime -->
        </colgroup>

        <thead>
            <tr>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Environment</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Date Variable</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Min Date</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Max Date</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Row Count</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Query Runtime</th>
            </tr>
        </thead>

        <tbody>
'''

    for row_section in row_sections:
        html += row_section

    html += '''        </tbody>
    </table>
'''

    return html


def create_column_stats_table(row_sections: List[str], vintage: str = 'month') -> str:
    """
    Wrap column stats comparison rows in a single table.

    Args:
        row_sections: List of HTML row strings from generate_column_stats_html()
        vintage: Vintage type (day/week/month/quarter/year) for header label

    Returns:
        Complete HTML table string
    """
    # Map vintage to plural label
    vintage_labels = {
        'day': '# days',
        'week': '# weeks',
        'month': '# months',
        'quarter': '# quarters',
        'year': '# years',
    }
    vintage_label = vintage_labels.get(vintage, '# vintages')

    html = f'''
    <table style="border-collapse:collapse; width:100%; font-family:Segoe UI, Arial, sans-serif; font-size:12.5px; table-layout:fixed;">
        <colgroup>
            <col style="width:120px;"> <!-- Environment -->
            <col style="width:140px;"> <!-- Date Variable -->
            <col style="width:110px;"> <!-- Min Date -->
            <col style="width:110px;"> <!-- Max Date -->
            <col style="width:100px;"> <!-- # Columns -->
            <col style="width:100px;"> <!-- # Vintages -->
            <col style="width:120px;"> <!-- Query Runtime -->
        </colgroup>

        <thead>
            <tr>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Environment</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Date Variable</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Min Date</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Max Date</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;"># Columns</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">{vintage_label}</th>
                <th style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;">Query Runtime</th>
            </tr>
        </thead>

        <tbody>
'''

    for row_section in row_sections:
        html += row_section

    html += '''        </tbody>
    </table>
'''

    return html


def wrap_html_document(
    title: str,
    sections: List[str],
    subtitle: Optional[str] = None,
) -> str:
    """
    Wrap HTML sections in a complete HTML document.

    Args:
        title: Document title
        sections: List of HTML section strings
        subtitle: Optional subtitle text

    Returns:
        Complete HTML document string
    """
    now = datetime.now().strftime('%b %d, %Y')

    css = '''
    <style>
        body {
            margin: 0;
            padding: 12px;
            font-family: Segoe UI, Arial, sans-serif;
            font-size: 12.5px;
        }

        summary::-webkit-details-marker {
            display: none;
        }

        details[open] summary::before {
            content: '▼ ';
        }

        details summary::before {
            content: '▶ ';
        }

        div.page-title {
            font-family: "Segoe UI", Arial, sans-serif;
            font-size: 28px;
            font-weight: 600;
            color: #1f2933;
            margin-bottom: 6px;
        }

        div.page-title .update-note {
            margin-top: 4px;
            font-size: 13px;
            color: #6b7280;
            font-weight: 400;
        }

        .calendar-icon {
            width: 14px;
            height: 14px;
            margin-right: 6px;
            vertical-align: -2px;
            stroke: #9CA3AF;
            stroke-width: 2;
            fill: none;
        }
    </style>
    '''

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    {css}
</head>
<body>
    <div class="page-title">
        {title}
        <div class="update-note">
            <svg class="calendar-icon" viewBox="0 0 24 24">
                <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
                <line x1="16" y1="2" x2="16" y2="6" />
                <line x1="8" y1="2" x2="8" y2="6" />
                <line x1="3" y1="10" x2="21" y2="10" />
            </svg>
            Latest update: {now}{f' | {subtitle}' if subtitle else ''}
        </div>
    </div>

'''

    # Join all sections into the body
    for section in sections:
        html += section

    html += '''
</body>
</html>
'''

    return html
