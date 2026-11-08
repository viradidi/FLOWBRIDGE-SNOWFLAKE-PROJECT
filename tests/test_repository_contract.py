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

    def test_streamlit_project_files_exist(self):
        dashboard = ROOT / "flowbridge_dashboard"
        self.assertTrue((dashboard / "streamlit_app.py").is_file())
        self.assertTrue((dashboard / "pyproject.toml").is_file())
        self.assertTrue((dashboard / ".streamlit" / "config.toml").is_file())

    def test_streamlit_source_parses(self):
        source = ROOT / "flowbridge_dashboard" / "streamlit_app.py"
        ast.parse(source.read_text(encoding="utf-8"), filename=str(source))

    def test_validation_commands_match_current_layout(self):
        guide = (ROOT / "docs" / "VALIDATION.md").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "validation.yml").read_text(encoding="utf-8")
        expected_compile_command = "python -m compileall -q flowbridge_dashboard"
        self.assertIn(expected_compile_command, guide)
        self.assertIn(expected_compile_command, workflow)
        self.assertIn("python -m pytest -q", guide)
        self.assertIn("python -m pytest -q", workflow)
        self.assertNotIn("compileall -q Untitled", guide)

    def test_documented_project_references_exist(self):
        for relative_path in (
            "docs/ARCHITECTURE.md",
            "docs/OPERATIONS.md",
            "docs/VALIDATION.md",
        ):
            with self.subTest(path=relative_path):
                self.assertTrue((ROOT / relative_path).is_file(), relative_path)

    def test_readme_documents_scope_and_metric_caveat(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()
        self.assertIn("inventory movement", readme)
        self.assertIn("proxy", readme)
        self.assertIn("snowflake", readme)
        self.assertIn("limitations", readme)
        self.assertIn("author-confirmed", readme)
        self.assertNotIn("fresh validation of the most recent repository edits is required", readme)


if __name__ == "__main__":
    unittest.main()