"""Unit tests for Settings configuration."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from dbxtop.config import Settings


class TestSettingsValidation:
    def test_valid_settings(self) -> None:
        s = Settings(profile="default", cluster_id="abc-123")
        assert s.fast_poll_s == 3.0
        assert s.slow_poll_s == 15.0
        assert s.theme == "dark"

    def test_empty_profile_raises(self) -> None:
        with pytest.raises(ValueError, match="profile"):
            Settings(profile="", cluster_id="abc")

    def test_empty_cluster_id_raises(self) -> None:
        with pytest.raises(ValueError, match="cluster_id"):
            Settings(profile="default", cluster_id="")

    def test_fast_poll_too_low(self) -> None:
        with pytest.raises(ValueError, match="fast_poll_s"):
            Settings(profile="p", cluster_id="c", fast_poll_s=0.5)

    def test_fast_poll_too_high(self) -> None:
        with pytest.raises(ValueError, match="fast_poll_s"):
            Settings(profile="p", cluster_id="c", fast_poll_s=31.0)

    def test_slow_poll_too_low(self) -> None:
        with pytest.raises(ValueError, match="slow_poll_s"):
            Settings(profile="p", cluster_id="c", slow_poll_s=4.0)

    def test_slow_less_than_fast(self) -> None:
        with pytest.raises(ValueError, match="slow_poll_s"):
            Settings(profile="p", cluster_id="c", fast_poll_s=10.0, slow_poll_s=5.0)

    def test_invalid_theme(self) -> None:
        with pytest.raises(ValueError, match="theme"):
            Settings(profile="p", cluster_id="c", theme="solarized")


class TestSettingsFromCli:
    def test_cli_values_take_precedence(self) -> None:
        with patch.dict(os.environ, {"DBXTOP_PROFILE": "env-profile"}):
            s = Settings.from_cli(profile="cli-profile", cluster_id="c1")
            assert s.profile == "cli-profile"

    def test_env_fallback(self) -> None:
        with patch.dict(os.environ, {"DBXTOP_PROFILE": "env-profile", "DBXTOP_CLUSTER_ID": "env-cluster"}):
            s = Settings.from_cli()
            assert s.profile == "env-profile"
            assert s.cluster_id == "env-cluster"

    def test_refresh_from_env(self) -> None:
        with patch.dict(os.environ, {"DBXTOP_REFRESH": "5.0", "DBXTOP_SLOW_REFRESH": "30.0"}):
            s = Settings.from_cli(profile="p", cluster_id="c")
            assert s.fast_poll_s == 5.0
            assert s.slow_poll_s == 30.0

    def test_theme_from_env(self) -> None:
        with patch.dict(os.environ, {"DBXTOP_THEME": "light"}):
            s = Settings.from_cli(profile="p", cluster_id="c")
            assert s.theme == "light"
