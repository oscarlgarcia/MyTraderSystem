from app.features.releases import FeatureReleaseRegistry


def test_release_registry_activate_and_rollback(tmp_path):
    reg = FeatureReleaseRegistry(tmp_path / "releases.json")
    reg.activate(name="default", version="1.0.0")
    reg.activate(name="default", version="1.1.0")
    rolled = reg.rollback(name="default")
    assert rolled.active_version == "1.0.0"
