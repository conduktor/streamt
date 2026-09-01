"""Import and argument-parsing smoke tests for every top-level CLI command."""

from click.testing import CliRunner

from streamt.cli import main


def test_every_top_level_command_has_working_help() -> None:
    runner = CliRunner()

    assert main.commands
    for command_name in sorted(main.commands):
        result = runner.invoke(main, [command_name, "--help"])
        assert result.exit_code == 0, f"{command_name}: {result.output} {result.exception!r}"
        assert "Usage:" in result.output
