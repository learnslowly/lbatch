import tempfile
import unittest
from pathlib import Path

from lbatch.sbatch_directives import extract_directive_argv


class DirectiveTests(unittest.TestCase):
    def test_stops_at_first_executable_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "job.batch"
            script.write_text("\n# comment\n#SBATCH --time=1:00\necho hi\n#SBATCH --time=2:00\n")
            self.assertEqual(extract_directive_argv(str(script)), ["--time=1:00"])


if __name__ == "__main__":
    unittest.main()
