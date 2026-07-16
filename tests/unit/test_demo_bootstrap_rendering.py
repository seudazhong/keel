from keel_core.config import Settings
from keel_worker.demo_bootstrap import describe_plan


def test_describe_plan_redacts_database_password() -> None:
    password = "super-secret"
    database_url = "postgresql+psycopg://demo:" + password + "@localhost:5432/keel_test"
    settings = Settings(
        app_env="test",
        database_url=database_url,
        redis_url="redis://localhost:6379/0",
    )

    plan = describe_plan(scope_id="web:local", mode="fake", settings=settings)

    assert password not in plan
    assert "@localhost:5432/keel_test" in plan
