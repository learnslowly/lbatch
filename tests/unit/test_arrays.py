import unittest

from lbatch.arrays import parse_array_spec


class ArrayTests(unittest.TestCase):
    def test_range_step_and_concurrency(self):
        exp = parse_array_spec("1-7:2%3")
        self.assertEqual(exp.task_ids, [1, 3, 5, 7])
        self.assertEqual(exp.concurrency_limit, 3)
        self.assertEqual(exp.count, 4)
        self.assertEqual(exp.minimum, 1)
        self.assertEqual(exp.maximum, 7)
        self.assertEqual(exp.step, 2)

    def test_list_deduplicates_in_order(self):
        self.assertEqual(parse_array_spec("1,3,3,5-6").task_ids, [1, 3, 5, 6])


if __name__ == "__main__":
    unittest.main()
