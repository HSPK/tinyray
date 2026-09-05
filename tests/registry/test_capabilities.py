"""注册中心得自报能做什么，而不是让客户端靠属性探测去猜。

猜不出来：老注册中心对长轮询请求的回答**又快又对**，只是不挂起而已，所以
"挂起了但什么都没发生"和"根本不会挂起"从客户端看一模一样。实测对着 v0.6.1：
每秒 14.5 次请求，而当前版本 0.12 次 —— 一百倍，`/health` 只说 `{"status":"ok"}`，
客户端也没有任何入口能问。

包版本 (`tinyray.__version__`) 说的是本地这一侧能做什么。注册中心是另一个进程，
可以独立升级，所以那是另一个问题，得单独有答案。
"""

from __future__ import annotations

import json
import urllib.request
import warnings

import pytest
import tinyray


def test_health_names_the_registry(registry):
    """不加入也要能看出版本，部署检查用得上。"""
    with urllib.request.urlopen(f"http://{registry.endpoint}/health", timeout=2) as r:
        body = json.loads(r.read())
    assert body["status"] == "ok"
    assert body["version"] == tinyray.__version__
    assert body["protocol"] >= 2


def test_a_member_can_ask_what_the_registry_can_do(registry):
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        info = m.registry
        assert info.version == tinyray.__version__
        assert info.protocol >= 2
        assert info.supports("long_poll") is True
        assert info.supports("publication_ordering") is True


def test_an_unknown_feature_is_an_error_not_a_false(registry):
    """猜错功能名要当场报错。返回 False 的话，写错一个名字就会安静地走进降级
    分支，而那正是这套机制要消灭的东西。"""
    with tinyray.join("p", slot=0, size=1) as m:
        m.ready()
        with pytest.raises(ValueError, match="no such feature"):
            m.registry.supports("teleportation")


def test_a_registry_too_old_to_say_reads_as_zero():
    """字段缺失必须解码成 0，而不是报错 —— 老注册中心根本不会发这个字段。"""
    assert tinyray.RegistryInfo(0, "").supports("long_poll") is False
    assert tinyray.RegistryInfo(1, "0.8.1").supports("long_poll") is True
    assert tinyray.RegistryInfo(1, "0.14.0").supports("publication_ordering") is False
    assert tinyray.RegistryInfo(2, "").supports("publication_ordering") is True


@pytest.mark.parametrize("feature", ["long_poll", "publication_ordering"])
def test_wanting_more_than_the_registry_has_says_so_instead_of_degrading_quietly(
    registry, monkeypatch, feature
):
    """降级是性能悬崖而不是报错，所以除了这条告警不会有任何东西说话。

    这里不去搭一个老注册中心，而是把要求抬到当前注册中心之上 —— 要验的是
    "本地要的比对面有的新时会不会出声"，那个比较跟具体差多少无关。字段缺失
    解码成 0 由 `crates/tinyray-proto/tests/wire.rs` 那条守着。
    """
    monkeypatch.setitem(tinyray.RegistryInfo.FEATURES, feature, 9999)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        m = tinyray.join("p", slot=0, size=1)
    try:
        kinds = [w.category for w in caught]
        assert tinyray.OldRegistryWarning in kinds, f"没有任何提示: {kinds}"
        said = str(next(w.message for w in caught if isinstance(w.message, Warning)))
        assert "9999" in said and registry.endpoint in said
        assert feature in said
        assert m.registry.supports(feature) is False
    finally:
        m.leave()


def test_a_current_registry_says_nothing(registry):
    """对偶：版本匹配时不能吵。一条每次 join 都出现的告警等于没有告警。"""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        m = tinyray.join("p", slot=0, size=1)
    try:
        assert tinyray.OldRegistryWarning not in [w.category for w in caught]
    finally:
        m.leave()
