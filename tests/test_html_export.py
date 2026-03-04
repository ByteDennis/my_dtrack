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

    def test_generate_perfect_match(self):
        """Test HTML generation for perfectly matching tables"""
        comparison = {
            'only_left': [],
            'only_right': [],
            'matching': [('2025-01-01', 100), ('2025-01-02', 150)],
            'mismatched': [],
            'summary': {
                'date_range_left': ('2025-01-01', '2025-01-02'),
                'date_range_right': ('2025-01-01', '2025-01-02'),
                'total_left': 250,
                'total_right': 250,
            }
        }

        html = generate_row_count_html(
            pair_name='test_pair',
            source_left='oracle',
            source_right='aws',
            table_left='test_oracle',
            table_right='test_aws',
            comparison=comparison
        )

        assert 'test_pair' in html
        assert '● 2 match' in html
        assert '○ 0 mismatch' in html
        assert 'oracle' in html
        assert 'aws' in html
        assert '<span class="match">0</span>' in html  # Row diff = 0

    def test_generate_with_mismatches(self):
        """Test HTML generation with mismatched counts"""
        comparison = {
            'only_left': [],
            'only_right': [],
            'matching': [('2025-01-01', 100)],
            'mismatched': [('2025-01-02', 150, 155)],
            'summary': {
                'date_range_left': ('2025-01-01', '2025-01-02'),
                'date_range_right': ('2025-01-01', '2025-01-02'),
                'total_left': 250,
                'total_right': 255,
            }
        }

        html = generate_row_count_html(
            pair_name='test_pair',
            source_left='pcds',
            source_right='aws',
            table_left='test_pcds',
            table_right='test_aws',
            comparison=comparison
        )

        assert '⚠ MISMATCH' in html
        assert '⚠ 1 mismatch' in html
        assert 'Details' in html
        assert '2025-01-02' in html
        assert '155' in html

    def test_generate_with_gaps(self):
        """Test HTML generation with date gaps"""
        comparison = {
            'only_left': [('2025-01-01', 100), ('2025-01-02', 150)],
            'only_right': [('2025-01-05', 200)],
            'matching': [],
            'mismatched': [],
            'summary': {
                'date_range_left': ('2025-01-01', '2025-01-02'),
                'date_range_right': ('2025-01-05', '2025-01-05'),
                'total_left': 250,
                'total_right': 200,
            }
        }

        html = generate_row_count_html(
            pair_name='test_pair',
            source_left='oracle',
            source_right='aws',
            table_left='test_oracle',
            table_right='test_aws',
            comparison=comparison
        )

        assert '2 oracle-only' in html
        assert '1 aws-only' in html
        assert 'Date Coverage Gap' in html
        assert 'oracle-only Dates' in html
        assert 'aws-only Dates' in html


class TestColumnStatsHTML:
    """Test column statistics HTML generation"""

    def test_generate_all_match(self):
        """Test HTML generation when all stats match"""
        comparison = {
            'amount': [{
                'dt': '2025-01-01',
                'col_type': 'numeric',
                'left_col': 'AMT',
                'right_col': 'amount',
                'n_total_left': 100,
                'n_total_right': 100,
                'n_total_diff': 0,
                'n_missing_left': 5,
                'n_missing_right': 5,
                'n_missing_diff': 0,
                'n_unique_left': 80,
                'n_unique_right': 80,
                'n_unique_diff': 0,
                'mean_left': 1500.0,
                'mean_right': 1500.0,
                'mean_diff': 0.0,
                'std_left': 200.0,
                'std_right': 200.0,
                'std_diff': 0.0,
                'min_left': '100',
                'min_right': '100',
                'max_left': '5000',
                'max_right': '5000',
            }]
        }

        html = generate_column_stats_html(
            pair_name='customer_stats',
            source_left='oracle',
            source_right='aws',
            table_left='stats_oracle',
            table_right='stats_aws',
            comparison=comparison,
            col_mappings={'AMT': 'amount'}
        )

        assert 'customer_stats' in html
        assert '1 vintages checked' in html
        assert '1 columns mapped' in html
        assert '● 1 match' in html
        assert '● 0 diff' in html

    def test_generate_with_differences(self):
        """Test HTML generation with statistical differences"""
        comparison = {
            'amount': [{
                'dt': '2025-01-01',
                'col_type': 'numeric',
                'left_col': 'AMT',
                'right_col': 'amount',
                'n_total_left': 100,
                'n_total_right': 95,
                'n_total_diff': -5,
                'n_missing_left': 5,
                'n_missing_right': 10,
                'n_missing_diff': 5,
                'n_unique_left': 80,
                'n_unique_right': 80,
                'n_unique_diff': 0,
                'mean_left': 1500.0,
                'mean_right': 1520.0,
                'mean_diff': 20.0,
                'std_left': 200.0,
                'std_right': 210.0,
                'std_diff': 10.0,
                'min_left': '100',
                'min_right': '100',
                'max_left': '5000',
                'max_right': '5000',
            }]
        }

        html = generate_column_stats_html(
            pair_name='customer_stats',
            source_left='oracle',
            source_right='aws',
            table_left='stats_oracle',
            table_right='stats_aws',
            comparison=comparison,
            col_mappings={'AMT': 'amount'}
        )

        assert '⚠ 1 diff' in html
        assert 'Vintages' in html
        assert 'amount' in html
        assert '2025-01-01' in html
        assert '[▶ Detail]' in html

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
                'n_unique_right': 3,
                'n_unique_diff': 0,
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

        assert _has_differences(comp_with_diff) is True

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
