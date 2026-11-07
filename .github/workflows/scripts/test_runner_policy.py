import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import runner_policy


class RunnerPolicyTests(unittest.TestCase):
    def test_load_allowlist_ignores_comments_and_case_normalizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "allowlist.txt"
            path.write_text(
                "julianknutsen\n"
                "  Csells  # maintainer\n"
                "\n"
                "# comment\n",
                encoding="utf-8",
            )

            self.assertEqual(runner_policy.load_allowlist(path), {"julianknutsen", "csells"})

    def test_pull_request_from_allowlisted_author_uses_blacksmith(self) -> None:
        use_blacksmith, reason, runners = runner_policy.select_runners(
            "pull_request",
            "Quad341",
            {"quad341"},
        )

        self.assertTrue(use_blacksmith)
        self.assertIn("every event", reason)
        self.assertEqual(runners["runner_32vcpu"], "blacksmith-32vcpu-ubuntu-2404")
        self.assertEqual(runners["runner_macos"], "blacksmith-6vcpu-macos-15")
        self.assertEqual(runners["runner_windows"], "blacksmith-4vcpu-windows-2025")

    def test_push_uses_blacksmith(self) -> None:
        use_blacksmith, reason, runners = runner_policy.select_runners(
            "push",
            "julianknutsen",
            {"julianknutsen"},
            force_blacksmith=False,
        )

        self.assertTrue(use_blacksmith)
        self.assertIn("every event", reason)
        self.assertEqual(runners["runner_32vcpu"], "blacksmith-32vcpu-ubuntu-2404")

    def test_forced_workflow_call_uses_blacksmith(self) -> None:
        use_blacksmith, reason, runners = runner_policy.select_runners(
            "workflow_call",
            "",
            set(),
            force_blacksmith=True,
        )

        self.assertTrue(use_blacksmith)
        self.assertIn("every event", reason)
        self.assertEqual(runners["runner_16vcpu"], "blacksmith-16vcpu-ubuntu-2404")
        self.assertEqual(runners["runner_macos"], "blacksmith-6vcpu-macos-15")
        self.assertEqual(runners["runner_windows"], "blacksmith-4vcpu-windows-2025")

    def test_unlisted_pull_request_author_uses_blacksmith(self) -> None:
        use_blacksmith, reason, runners = runner_policy.select_runners(
            "pull_request",
            "external-contributor",
            {"julianknutsen"},
            force_blacksmith=False,
        )

        self.assertTrue(use_blacksmith)
        self.assertIn("every event", reason)
        self.assertEqual(runners["runner_macos"], "blacksmith-6vcpu-macos-15")
        self.assertEqual(runners["runner_windows"], "blacksmith-4vcpu-windows-2025")


    def test_fork_repository_uses_github_hosted_runners(self) -> None:
        use_blacksmith, reason, runners = runner_policy.select_runners(
            "push",
            "zook-bot",
            set(),
            repository="zookanalytics/gascity",
        )

        self.assertFalse(use_blacksmith)
        self.assertIn("GitHub-hosted", reason)
        self.assertIn("zookanalytics/gascity", reason)
        self.assertEqual(runners, runner_policy.GITHUB_RUNNERS)
        self.assertEqual(runners["runner_32vcpu"], "ubuntu-latest")
        self.assertEqual(runners["runner_macos"], "macos-15")
        self.assertEqual(runners["runner_windows"], "windows-latest")

    def test_fork_pull_request_from_allowlisted_author_uses_github_hosted_runners(self) -> None:
        use_blacksmith, _, runners = runner_policy.select_runners(
            "pull_request",
            "Quad341",
            {"quad341"},
            repository="quad341/gascity",
        )

        self.assertFalse(use_blacksmith)
        self.assertEqual(runners, runner_policy.GITHUB_RUNNERS)

    def test_fork_repository_ignores_forced_blacksmith(self) -> None:
        use_blacksmith, _, runners = runner_policy.select_runners(
            "workflow_call",
            "",
            set(),
            force_blacksmith=True,
            repository="zookanalytics/gascity",
        )

        self.assertFalse(use_blacksmith)
        self.assertEqual(runners, runner_policy.GITHUB_RUNNERS)

    def test_canonical_repository_matches_case_insensitively(self) -> None:
        use_blacksmith, _, runners = runner_policy.select_runners(
            "push",
            "",
            set(),
            repository="GastownHall/GasCity",
        )

        self.assertTrue(use_blacksmith)
        self.assertEqual(runners, runner_policy.BLACKSMITH_RUNNERS)

    def test_main_selects_runners_for_the_workflow_repository(self) -> None:
        for repository, use_blacksmith, runner_2vcpu in (
            ("gastownhall/gascity", "true", "blacksmith-2vcpu-ubuntu-2404"),
            ("zookanalytics/gascity", "false", "ubuntu-latest"),
        ):
            with self.subTest(repository=repository), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "github_output"
                env = {
                    "EVENT_NAME": "push",
                    "GITHUB_REPOSITORY": repository,
                    "GITHUB_OUTPUT": str(output),
                }
                with mock.patch.dict(os.environ, env, clear=True):
                    runner_policy.main()

                lines = output.read_text(encoding="utf-8").splitlines()
                self.assertIn(f"use_blacksmith={use_blacksmith}", lines)
                self.assertIn(f"runner_2vcpu={runner_2vcpu}", lines)

    def test_main_requires_the_workflow_repository(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "github_output"
            env = {"EVENT_NAME": "push", "GITHUB_OUTPUT": str(output)}
            with mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaises(KeyError):
                    runner_policy.main()

            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
