from pathlib import Path
import ast
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_numbered_deployment_scripts_exist(self):
        expected = [
            "01_account_setup.sql",
            "02_bronze.sql",
            "03_silver_stage1.sql",
            "04_silver_stage2.sql",
            "05_gold.sql",
            "06_serving.sql",
            "07_governance.sql",
            "08_dev_prod.sql",
        ]
        for relative_path in expected:
            with self.subTest(path=relative_path):
                self.assertTrue((ROOT / relative_path).is_file(), relative_path)

    def test_streamlit_entrypoint_exists(self):
        self.assertTrue((ROOT / "Untitled" / "streamlit_app.py").is_file())

    def test_streamlit_source_parses(self):
        source = ROOT / "Untitled" / "streamlit_app.py"
        ast.parse(source.read_text(encoding="utf-8"), filename=str(source))

    def test_readme_documents_scope_and_metric_caveat(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()
        self.assertIn("inventory movement", readme)
        self.assertIn("proxy", readme)
        self.assertIn("snowflake", readme)
        self.assertIn("limitations", readme)


if __name__ == "__main__":
    unittest.main()
