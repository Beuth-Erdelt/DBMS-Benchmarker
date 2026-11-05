"""
Tests for converting values of result sets before they are compared.

convert_to_rounded_float_2() turns a value into a rounded float when it is a
single number, and leaves every other value as it is. TPC-DS item
descriptions are random words, e.g. "False," at SF 10, which Python reads as
the tuple (False,).

Run from the repository root: python -m unittest discover tests
"""
import unittest
from unittest import mock

from dbmsbenchmarker import benchmarker, tools


def _query_config(precision=0):
    return {'title': 'Q', 'query': 'SELECT 1',
            'timer': {'datatransfer': {'active': True, 'sorted': True, 'compare': 'result',
                                       'store': [], 'precision': precision}}}


def _single_result(rows, columnnames=('c1', 'c2')):
    inputs = [benchmarker.singleResultInput(0, rows, list(columnnames), _query_config())]
    return benchmarker.singleResult({'name': 'test'}, inputs, [0], 'test', 1)[0]


class ConvertToRoundedFloatTest(unittest.TestCase):
    """Single numbers become rounded floats, everything else stays text."""

    def test_numbers(self):
        self.assertEqual(tools.convert_to_rounded_float_2('12.345', 2), 12.35)
        self.assertEqual(tools.convert_to_rounded_float_2('7', 2), 7.0)
        self.assertEqual(tools.convert_to_rounded_float_2('1e3', 2), 1000.0)
        self.assertEqual(tools.convert_to_rounded_float_2(3.14159, 2), 3.14)

    def test_bool_equals_number(self):
        # a DBMS answering true and one answering 1 give the same result
        self.assertEqual(tools.convert_to_rounded_float_2('True', 2), 1.0)
        self.assertEqual(tools.convert_to_rounded_float_2('False', 2), 0.0)
        self.assertEqual(tools.convert_to_rounded_float_2('True', 2), tools.convert_to_rounded_float_2('1', 2))

    def test_other_literals_stay_text(self):
        for text in ('False,', '1, 2', '(1, 2)', '[1]', '{1: 2}', '1j', "'abc'", 'b"x"'):
            with self.subTest(text=text):
                self.assertEqual(tools.convert_to_rounded_float_2(text, 2), text)

    def test_text_and_none(self):
        self.assertEqual(tools.convert_to_rounded_float_2('Sports', 2), 'Sports')
        self.assertEqual(tools.convert_to_rounded_float_2('None', 2), 'None')


class SingleResultTest(unittest.TestCase):
    """Processing a result set: values like "False," and errors of the processing."""

    def test_description_like_a_tuple(self):
        result = _single_result([['AAAAAAAAKOBBAAAA', 'False,'], ['AAAAAAAAMOBBAAAA', 'Sports']])
        self.assertEqual(result.error, '')
        self.assertIn(['AAAAAAAAKOBBAAAA', 'False,'], result.data)

    def test_error_is_reported(self):
        with mock.patch.object(tools, 'convert_to_rounded_float_2', side_effect=TypeError('boom')):
            result = _single_result([['a', 'b']])
        self.assertIn('processing the result set failed (TypeError): boom', result.error)
        self.assertEqual(result.data, [])


if __name__ == '__main__':
    unittest.main()
