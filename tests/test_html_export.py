"""Tests for HTML export functionality"""

import pytest
from dtrack.html_export import (
    generate_row_count_html,
    generate_column_stats_html,
    wrap_html_document,
    _has_differences,
    _get_worst_stat,
)


class TestRowCountHTML:
    """Test row count HTML generation"""

    pass


class TestColumnStatsHTML:
    """Test column statistics HTML generation"""

    def test_generate_categorical_column(self):
        """Test HTML generation for categorical column"""
        comparison = {
            'status': [{
                'dt': '2025-01-01',
                'col_type': 'categorical',
                'left_col': 'STATUS',
                'right_col': 'status',
                'n_total_left': 100,
                'n_total_right': 100,
                'n_total_diff': 0,
                'n_missing_left': 0,
                'n_missing_right': 0,
                'n_missing_diff': 0,
                'n_unique_left': 3,
                'n_unique_right': 4,
                'n_unique_diff': 1,
                'min_left': 'ACTIVE',
                'min_right': 'ACTIVE',
                'max_left': 'INACTIVE',
                'max_right': 'INACTIVE',
                'top_10_left': '{"ACTIVE": 80, "INACTIVE": 15, "PENDING": 5}',
                'top_10_right': '{"ACTIVE": 75, "INACTIVE": 20, "PENDING": 5}',
            }]
        }

        html = generate_column_stats_html(
            pair_name='customer_stats',
            source_left='oracle',
            source_right='aws',
            table_left='stats_oracle',
            table_right='stats_aws',
            comparison=comparison,
            col_mappings={'STATUS': 'status'}
        )

        assert 'status' in html
        assert 'categorical' in html


class TestUtilityFunctions:
    """Test utility functions"""

    def test_has_differences_numeric(self):
        """Test detecting differences in numeric stats"""
        comp_with_diff = {
            'col_type': 'numeric',
            'n_total_diff': -5,
            'n_missing_diff': 0,
            'n_unique_diff': 0,
            'mean_diff': 20.0,
        }

        assert _has_differences(comp_with_diff) is True

        comp_no_diff = {
            'col_type': 'numeric',
            'n_total_diff': 0,
            'n_missing_diff': 0,
            'n_unique_diff': 0,
            'mean_diff': 0.0,
        }

        assert _has_differences(comp_no_diff) is False

    def test_has_differences_categorical(self):
        """Test detecting differences in categorical stats"""
        comp_with_diff = {
            'col_type': 'categorical',
            'n_total_diff': 0,
            'n_missing_diff': 0,
            'n_unique_diff': 0,
            'top_10_left': '{"A": 10}',
            'top_10_right': '{"A": 15}',
        }

        # top_10 differences alone no longer count as stat differences
        assert _has_differences(comp_with_diff) is False

    def test_get_worst_stat(self):
        """Test identifying worst stat"""
        comp = {
            'col_type': 'numeric',
            'n_total_left': 100,
            'n_total_diff': -5,
            'n_missing_diff': 0,
            'mean_left': 1500.0,
            'mean_diff': 0.0,
        }

        worst = _get_worst_stat(comp)
        assert 'n_total' in worst

        comp2 = {
            'col_type': 'numeric',
            'n_total_diff': 0,
            'n_missing_diff': 10,
        }

        worst2 = _get_worst_stat(comp2)
        assert 'n_missing' in worst2


class TestHTMLDocument:
    """Test HTML document wrapping"""

    def test_wrap_html_document(self):
        """Test wrapping sections in complete HTML document"""
        sections = [
            '<div class="section">Section 1</div>',
            '<div class="section">Section 2</div>',
        ]

        html = wrap_html_document(
            title='Test Report',
            sections=sections,
            subtitle='Test Subtitle'
        )

        assert '<!DOCTYPE html>' in html
        assert '<title>Test Report</title>' in html
        assert 'Test Subtitle' in html
        assert 'Section 1' in html
        assert 'Section 2' in html
        assert '<style>' in html  # CSS included

    def test_wrap_without_subtitle(self):
        """Test wrapping without subtitle"""
        sections = ['<div>Content</div>']

        html = wrap_html_document(
            title='Simple Report',
            sections=sections
        )

        assert '<title>Simple Report</title>' in html
        assert 'Content' in html
