"""Static checks on docker-compose.yml: the invariants that keep the VM deploy safe."""
import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
SERVICES = COMPOSE["services"]
ENV_EXAMPLE = (ROOT / ".env.example").read_text()


class ComposeTests(unittest.TestCase):
    def test_no_floating_tags(self):
        for name, svc in SERVICES.items():
            image = svc.get("image", "")
            if "build" in svc:
                continue
            self.assertNotRegex(image, r":latest$", f"{name} uses :latest")
            self.assertIn(":", image, f"{name} has no tag")

    def test_ports_follow_bind_address(self):
        for name, svc in SERVICES.items():
            for port in svc.get("ports", []):
                self.assertTrue(str(port).startswith("${BIND_ADDRESS:-127.0.0.1}:"), f"{name}: {port}")

    def test_default_bind_address_is_loopback(self):
        self.assertRegex(ENV_EXAMPLE, r"(?m)^BIND_ADDRESS=127\.0\.0\.1$")

    def test_depends_on_references_existing_services(self):
        for name, svc in SERVICES.items():
            deps = svc.get("depends_on", {})
            for dep in (deps if isinstance(deps, list) else deps.keys()):
                self.assertIn(dep, SERVICES, f"{name} -> {dep}")

    def test_build_contexts_and_dockerfiles_exist(self):
        for name, svc in SERVICES.items():
            build = svc.get("build")
            if not build:
                continue
            ctx = build if isinstance(build, str) else build["context"]
            df = "Dockerfile" if isinstance(build, str) else build.get("dockerfile", "Dockerfile")
            self.assertTrue((ROOT / ctx / df).is_file(), f"{name}: {ctx}/{df} missing")

    def test_secrets_are_required_not_defaulted(self):
        text = (ROOT / "docker-compose.yml").read_text()
        for var in ["POSTGRES_PASSWORD", "S3_SECRET_KEY", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET", "AIRFLOW_ADMIN_PASSWORD"]:
            self.assertRegex(text, rf"\$\{{{var}:\?", f"{var} must use the :? required form")
            self.assertNotRegex(text, rf"\$\{{{var}:-", f"{var} must not have a default")

    def test_every_env_example_token_is_known_to_init_script(self):
        script = (ROOT / "scripts" / "init-env.sh").read_text()
        for token in set(re.findall(r"__[A-Z_]+__", ENV_EXAMPLE)):
            self.assertIn(token, script, f"init-env.sh cannot fill {token}")

    def test_no_zookeeper_with_kafka_4(self):
        self.assertNotIn("zookeeper", SERVICES)
        self.assertTrue(SERVICES["kafka"]["image"].startswith("apache/kafka:4."))

    def test_one_shot_jobs_do_not_restart(self):
        for name in ["airflow-init", "snowflake-init", "connector-init"]:
            self.assertEqual(SERVICES[name]["restart"], "no", name)

    def test_postgres_is_configured_for_cdc(self):
        cmd = SERVICES["postgres"]["command"]
        self.assertIn("wal_level=logical", cmd)
        self.assertIn("max_slot_wal_keep_size", cmd)
        mounts = " ".join(SERVICES["postgres"]["volumes"])
        self.assertIn("schema.sql:/docker-entrypoint-initdb.d/", mounts)


if __name__ == "__main__":
    unittest.main()
