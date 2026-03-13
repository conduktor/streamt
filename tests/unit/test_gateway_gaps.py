"""Gap tests for GatewayDeployer: interceptor change detection, orphaning,
partial failure, plan/apply/delete behavior, and plugin config generation.

Groups covered:
  3. Gateway interceptor change detection
  4. Gateway interceptor orphaning
  6. Gateway alias + interceptor partial failure
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streamt.compiler.manifest import GatewayRuleArtifact
from streamt.deployer.gateway import GatewayDeployer

# ===========================================================================
# GROUP 3: Gateway interceptor change detection
# ===========================================================================


class TestGatewayInterceptorChangeDetection:
    """plan() should detect interceptor config changes, not just
    physical_topic changes."""

    @pytest.fixture
    def deployer(self):
        d = GatewayDeployer.__new__(GatewayDeployer)
        d.admin_url = "http://gw:8888"
        d.auth = None
        d.virtual_cluster = None
        d._session = MagicMock()
        d._closed = False
        return d

    def test_different_interceptor_count_should_detect_change(self, deployer):
        """Alias same physical_topic, but 3 interceptors instead of 1."""
        with patch.object(deployer, "get_alias_topic") as mock_alias, \
             patch.object(deployer, "list_interceptors") as mock_ints:
            mock_alias.return_value = {
                "metadata": {"name": "my_vt"},
                "spec": {"physicalName": "my_physical"},
            }
            mock_ints.return_value = [
                {"metadata": {"name": "rule1_filter_0"}},
            ]
            artifact = GatewayRuleArtifact(
                name="rule1", virtual_topic="my_vt", physical_topic="my_physical",
                interceptors=[
                    {"type": "filter", "config": {"where": "a = 1"}},
                    {"type": "mask", "config": {"field": "ssn"}},
                    {"type": "encrypt", "config": {"field": "cc"}},
                ],
            )
            change = deployer.plan(artifact)
            assert change.action == "update"

    def test_different_interceptor_config_should_detect_change(self, deployer):
        """Same count, different config on the interceptor."""
        with patch.object(deployer, "get_alias_topic") as mock_alias, \
             patch.object(deployer, "list_interceptors") as mock_ints:
            mock_alias.return_value = {
                "metadata": {"name": "my_vt"},
                "spec": {"physicalName": "my_physical"},
            }
            mock_ints.return_value = [{
                "metadata": {"name": "rule1_filter_0"},
                "spec": {"config": {"where": "status = 'active'"}},
            }]
            artifact = GatewayRuleArtifact(
                name="rule1", virtual_topic="my_vt", physical_topic="my_physical",
                interceptors=[{"type": "filter", "config": {"where": "status = 'inactive'"}}],
            )
            change = deployer.plan(artifact)
            assert change.action == "update"

    def test_same_physical_no_interceptors_returns_none(self, deployer):
        """Same physical_topic, no interceptors on either side -> none."""
        with patch.object(deployer, "get_alias_topic") as mock_alias:
            mock_alias.return_value = {
                "metadata": {"name": "my_vt"},
                "spec": {"physicalName": "my_physical"},
            }
            artifact = GatewayRuleArtifact(
                name="rule1", virtual_topic="my_vt", physical_topic="my_physical",
                interceptors=[],
            )
            assert deployer.plan(artifact).action == "none"

    def test_different_physical_topic_returns_update(self, deployer):
        with patch.object(deployer, "get_alias_topic") as mock_alias:
            mock_alias.return_value = {
                "metadata": {"name": "my_vt"},
                "spec": {"physicalName": "old_physical"},
            }
            artifact = GatewayRuleArtifact(
                name="rule1", virtual_topic="my_vt", physical_topic="new_physical",
            )
            change = deployer.plan(artifact)
            assert change.action == "update"
            assert change.changes["physical_topic"]["from"] == "old_physical"
            assert change.changes["physical_topic"]["to"] == "new_physical"


# ===========================================================================
# GROUP 4: Gateway interceptor orphaning
# ===========================================================================


class TestGatewayInterceptorOrphaning:
    """apply() with fewer interceptors should delete the orphaned ones."""

    @pytest.fixture
    def deployer(self):
        d = GatewayDeployer.__new__(GatewayDeployer)
        d.admin_url = "http://gw:8888"
        d.auth = None
        d.virtual_cluster = None
        d._session = MagicMock()
        d._closed = False
        return d

    def test_fewer_interceptors_deletes_orphans(self, deployer):
        """3 interceptors exist, artifact has 1 -> 2 should be deleted."""
        with patch.object(deployer, "get_alias_topic") as mock_get, \
             patch.object(deployer, "create_alias_topic"), \
             patch.object(deployer, "create_interceptor"), \
             patch.object(deployer, "list_interceptors") as mock_list, \
             patch.object(deployer, "delete_interceptor") as mock_del:
            mock_get.return_value = {"metadata": {"name": "vt"}, "spec": {"physicalName": "pt"}}
            mock_list.return_value = [
                {"metadata": {"name": "r_filter_0"}},
                {"metadata": {"name": "r_mask_1"}},
                {"metadata": {"name": "r_encrypt_2"}},
            ]
            artifact = GatewayRuleArtifact(
                name="r", virtual_topic="vt", physical_topic="pt",
                interceptors=[{"type": "filter", "config": {"where": "x = 1"}}],
            )
            deployer.apply(artifact)
            deleted = [c.args[0] for c in mock_del.call_args_list]
            assert "r_mask_1" in deleted
            assert "r_encrypt_2" in deleted

    def test_zero_interceptors_deletes_all(self, deployer):
        """2 interceptors exist, artifact has 0 -> all should be deleted."""
        with patch.object(deployer, "get_alias_topic") as mock_get, \
             patch.object(deployer, "create_alias_topic"), \
             patch.object(deployer, "list_interceptors") as mock_list, \
             patch.object(deployer, "delete_interceptor") as mock_del:
            mock_get.return_value = {"metadata": {"name": "vt"}, "spec": {"physicalName": "pt"}}
            mock_list.return_value = [
                {"metadata": {"name": "r_filter_0"}},
                {"metadata": {"name": "r_mask_1"}},
            ]
            artifact = GatewayRuleArtifact(
                name="r", virtual_topic="vt", physical_topic="pt", interceptors=[],
            )
            deployer.apply(artifact)
            assert mock_del.call_count == 2


# ===========================================================================
# GROUP 6: Gateway alias + interceptor partial failure
# ===========================================================================


class TestGatewayPartialFailure:

    @pytest.fixture
    def deployer(self):
        d = GatewayDeployer.__new__(GatewayDeployer)
        d.admin_url = "http://gw:8888"
        d.auth = None
        d.virtual_cluster = None
        d._session = MagicMock()
        d._closed = False
        return d

    def test_interceptor_failure_after_alias_creation(self, deployer):
        """Alias succeeds, interceptor raises -> rollback alias and re-raise."""
        with patch.object(deployer, "get_alias_topic", return_value=None), \
             patch.object(deployer, "list_interceptors", return_value=[]), \
             patch.object(deployer, "create_alias_topic") as mock_alias, \
             patch.object(deployer, "create_interceptor") as mock_int, \
             patch.object(deployer, "delete_alias_topic") as mock_del_alias:
            mock_alias.return_value = {"ok": True}
            mock_int.side_effect = RuntimeError("Gateway 500")
            artifact = GatewayRuleArtifact(
                name="r1", virtual_topic="vt1", physical_topic="pt1",
                interceptors=[{"type": "filter", "config": {"where": "x = 1"}}],
            )
            with pytest.raises(RuntimeError, match="500"):
                deployer.apply(artifact)
            mock_alias.assert_called_once()
            mock_del_alias.assert_called_once_with("vt1")


# ===========================================================================
# Happy-path apply/plan/delete tests
# ===========================================================================


class TestGatewayApplyHappyPath:

    @pytest.fixture
    def deployer(self):
        d = GatewayDeployer.__new__(GatewayDeployer)
        d.admin_url = "http://gw:8888"
        d.auth = None
        d.virtual_cluster = None
        d._session = MagicMock()
        d._closed = False
        return d

    def test_creates_alias_and_interceptors(self, deployer):
        with patch.object(deployer, "get_alias_topic", return_value=None), \
             patch.object(deployer, "create_alias_topic") as mock_alias, \
             patch.object(deployer, "create_interceptor") as mock_int:
            artifact = GatewayRuleArtifact(
                name="r1", virtual_topic="vt1", physical_topic="pt1",
                interceptors=[
                    {"type": "filter", "config": {"where": "status = 'a'"}},
                    {"type": "mask", "config": {"field": "ssn", "method": "MASK_ALL"}},
                ],
            )
            result = deployer.apply(artifact)
            assert result == "created"
            mock_alias.assert_called_once_with(name="vt1", physical_topic="pt1")
            assert mock_int.call_count == 2

    def test_returns_updated_when_alias_exists(self, deployer):
        with patch.object(deployer, "get_alias_topic") as mock_get, \
             patch.object(deployer, "create_alias_topic"), \
             patch.object(deployer, "create_interceptor"):
            mock_get.return_value = {"metadata": {"name": "vt1"}, "spec": {"physicalName": "pt1"}}
            artifact = GatewayRuleArtifact(
                name="r1", virtual_topic="vt1", physical_topic="pt1", interceptors=[],
            )
            assert deployer.apply(artifact) == "updated"

    def test_skips_unknown_interceptor_types(self, deployer):
        with patch.object(deployer, "get_alias_topic", return_value=None), \
             patch.object(deployer, "create_alias_topic"), \
             patch.object(deployer, "create_interceptor") as mock_int:
            artifact = GatewayRuleArtifact(
                name="r1", virtual_topic="vt1", physical_topic="pt1",
                interceptors=[
                    {"type": "nonexistent", "config": {}},
                    {"type": "filter", "config": {"where": "x = 1"}},
                ],
            )
            deployer.apply(artifact)
            assert mock_int.call_count == 1

    def test_plan_new_rule_returns_create(self, deployer):
        with patch.object(deployer, "get_alias_topic", return_value=None):
            artifact = GatewayRuleArtifact(name="r1", virtual_topic="vt", physical_topic="pt")
            change = deployer.plan(artifact)
            assert change.action == "create"
            assert change.desired is artifact

    def test_delete_removes_alias_and_related_interceptors(self, deployer):
        with patch.object(deployer, "delete_alias_topic", return_value=True) as mock_da, \
             patch.object(deployer, "list_interceptors") as mock_li, \
             patch.object(deployer, "delete_interceptor") as mock_di:
            mock_li.return_value = [
                {"metadata": {"name": "r1_filter_0"}},
                {"metadata": {"name": "r1_mask_1"}},
                {"metadata": {"name": "other_filter_0"}},
            ]
            assert deployer.delete("r1") is True
            mock_da.assert_called_once_with("r1")
            deleted = [c.args[0] for c in mock_di.call_args_list]
            assert "r1_filter_0" in deleted
            assert "r1_mask_1" in deleted
            assert "other_filter_0" not in deleted


class TestGatewayBuildPluginConfig:

    @pytest.fixture
    def deployer(self):
        return GatewayDeployer.__new__(GatewayDeployer)

    def test_filter(self, deployer):
        a = GatewayRuleArtifact(name="r", virtual_topic="vt", physical_topic="my-topic")
        cfg = deployer._build_plugin_config("filter", {"where": "status = 'a'"}, a)
        assert cfg["virtualTopic"] == "vt"
        assert '"my-topic"' in cfg["statement"]
        assert "status = 'a'" in cfg["statement"]

    def test_mask(self, deployer):
        a = GatewayRuleArtifact(name="r", virtual_topic="vt", physical_topic="pt")
        cfg = deployer._build_plugin_config("mask", {"field": "ssn", "method": "MASK_FIRST_N"}, a)
        assert cfg["policies"][0]["fields"] == ["ssn"]
        assert cfg["policies"][0]["rule"]["type"] == "MASK_FIRST_N"

    def test_encrypt(self, deployer):
        a = GatewayRuleArtifact(name="r", virtual_topic="vt", physical_topic="pt")
        cfg = deployer._build_plugin_config("encrypt", {"field": "cc", "algorithm": "AES128_GCM"}, a)
        assert cfg["fields"] == ["cc"]
        assert cfg["algorithm"] == "AES128_GCM"

    def test_unknown_returns_raw(self, deployer):
        a = GatewayRuleArtifact(name="r", virtual_topic="vt", physical_topic="pt")
        assert deployer._build_plugin_config("custom", {"k": "v"}, a) == {"k": "v"}
