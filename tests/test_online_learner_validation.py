"""Input validation for OnlineLearner.learn_one — rejects poisoning inputs."""

import math

import pytest


@pytest.fixture
def learner(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "ml.serving.online_learner.ARTIFACTS_DIR", tmp_path,
    )
    monkeypatch.setattr(
        "ml.serving.online_learner.MODEL_PATH", tmp_path / "online_learner.pkl",
    )
    from ml.serving.online_learner import OnlineLearner
    return OnlineLearner()


def _valid_features():
    return {
        "spread_abs": 1.2,
        "spread_pct": 0.001,
        "price_a": 30000.0,
        "price_b": 30010.0,
    }


def test_learn_one_accepts_valid_input(learner):
    learner.learn_one(_valid_features(), 1)
    assert learner._update_count == 1


def test_learn_one_rejects_non_dict_features(learner):
    with pytest.raises((TypeError, ValueError)):
        learner.learn_one([1, 2, 3], 1)


def test_learn_one_rejects_nan_feature(learner):
    f = _valid_features()
    f["spread_abs"] = float("nan")
    with pytest.raises(ValueError, match="NaN|finite"):
        learner.learn_one(f, 1)


def test_learn_one_rejects_inf_feature(learner):
    f = _valid_features()
    f["price_a"] = float("inf")
    with pytest.raises(ValueError, match="Inf|finite"):
        learner.learn_one(f, 1)


def test_learn_one_rejects_non_numeric_feature(learner):
    f = _valid_features()
    f["spread_abs"] = "nope"
    with pytest.raises((TypeError, ValueError)):
        learner.learn_one(f, 1)


def test_learn_one_rejects_invalid_label(learner):
    with pytest.raises(ValueError, match="label"):
        learner.learn_one(_valid_features(), 2)


def test_learn_one_rejects_non_int_label(learner):
    with pytest.raises((TypeError, ValueError)):
        learner.learn_one(_valid_features(), "1")


def test_save_state_is_atomic_via_replace(learner, tmp_path, monkeypatch):
    """_save_state must write through a temp file and os.replace."""
    import ml.serving.online_learner as mod

    replace_called = {"count": 0}
    original_replace = mod.os.replace

    def spy_replace(src, dst):
        replace_called["count"] += 1
        return original_replace(src, dst)

    monkeypatch.setattr(mod.os, "replace", spy_replace)
    learner._save_state()
    assert replace_called["count"] == 1
    assert (tmp_path / "online_learner.pkl").exists()
