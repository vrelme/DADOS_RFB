from app.config import Settings

def test_app_name():
    assert Settings.APP_NAME is not None