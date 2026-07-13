import unittest

from lbatch.parser import get_option, parse_submission


class ParserTests(unittest.TestCase):
    def test_mixed_dependency_splits_local_and_external(self):
        sub = parse_submission(["--dependency=afterany:123456:lb:g000001", "job.batch"])
        self.assertEqual(sub.local_dependencies, [("afterany", "lb:g000001")])
        self.assertEqual(get_option(sub.sbatch_options, "--dependency"), "afterany:123456")

    def test_lbatch_afterok(self):
        sub = parse_submission(["--lbatch-afterok", "lb:g000002", "--array", "1-2", "job.batch", "x"])
        self.assertEqual(sub.local_dependencies, [("afterok", "lb:g000002")])
        self.assertEqual(get_option(sub.sbatch_options, "--array"), "1-2")
        self.assertEqual(sub.script_args, ["x"])


if __name__ == "__main__":
    unittest.main()
