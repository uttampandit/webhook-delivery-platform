#!/usr/bin/env python3
"""Exercise the local Compose API and receiver path without printing secrets."""

import json
import os
import re
import subprocess
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class SmokeFailure(Exception):
    pass


def run(args, *, check=True, timeout=30):
    result = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if check and result.returncode:
        raise SmokeFailure("command failed: " + " ".join(args[:3]))
    return result


def compose(*args, check=True, timeout=30):
    return run(["docker", "compose", *args], check=check, timeout=timeout)


def api_get(path, authenticated=False):
    # Run curl inside the API container. Some Docker Desktop contexts expose
    # published ports only to the native host, not this process namespace.
    command = (
        'if [ "$2" = "yes" ]; then '
        'printf "header = \\"Authorization: Bearer %s\\"\\n" "$API_TOKEN" | '
        'curl --config - --silent --show-error --header "Accept: text/html,*/*" '
        '--output - --write-out "\\n%{http_code}" "$1"; '
        'else curl --silent --show-error --header "Accept: text/html,*/*" '
        '--output - --write-out "\\n%{http_code}" "$1"; fi'
    )
    result = compose(
        "exec", "-T", "api", "sh", "-c", command, "sh",
        "http://127.0.0.1:8080" + path,
        "yes" if authenticated else "no",
    )
    body, status = result.stdout.rsplit("\n", 1)
    return int(status), body.encode("utf-8")


def validate_compose_config():
    config = json.loads(compose("config", "--format", "json", timeout=30).stdout)
    services = config.get("services", {})
    required = {"postgres", "init-compose-db", "migrate", "seed", "api", "worker", "fulfillment", "notification"}
    if not required.issubset(services):
        raise SmokeFailure("Compose service set is incomplete")
    for name, service in services.items():
        ports = service.get("ports", [])
        if name in {"api", "postgres"}:
            expected = 8080 if name == "api" else 5432
            if len(ports) != 1 or ports[0].get("target") != expected or ports[0].get("host_ip") != "127.0.0.1":
                raise SmokeFailure(name + " must publish exactly one loopback-only port")
        elif ports:
            raise SmokeFailure(name + " unexpectedly publishes a host port")
        build = service.get("build")
        if build and (build.get("context") != ROOT or os.path.basename(build.get("dockerfile", "Dockerfile")) != "Dockerfile"):
            raise SmokeFailure("application image build context is unexpected")
    api_env = services["api"].get("environment", {})
    token = api_env.get("API_TOKEN", "")
    if not token:
        raise SmokeFailure("API token is missing from resolved Compose configuration")
    return config, services["api"]["ports"][0]["published"], token


def assert_image_is_private():
    image = run(["docker", "image", "inspect", "webhook-platform:local"], timeout=15)
    metadata = json.loads(image.stdout)[0]
    config = metadata.get("Config", {})
    if str(config.get("User", "")) not in {"10001", "10001:10001"}:
        raise SmokeFailure("application image is not configured for the non-root runtime user")
    env = "\n".join(config.get("Env", []))
    for secret_name in ("API_TOKEN", "DEMO_CONTROL_TOKEN", "FULFILLMENT_SIGNING_KEY_V1", "NOTIFICATION_SIGNING_KEY_V1", "POSTGRES_PASSWORD"):
        if secret_name in env:
            raise SmokeFailure("application image contains a runtime secret setting")
    with open(os.path.join(ROOT, ".dockerignore"), encoding="utf-8") as stream:
        ignored = set(stream.read().splitlines())
    if not {".env", ".env.*", "state/"}.issubset(ignored):
        raise SmokeFailure("Docker build context does not exclude local secrets and state")


def assert_runtime_bindings(config):
    services = config["services"]
    for name in services:
        container_id = compose("ps", "--all", "-q", name).stdout.strip()
        if not container_id:
            raise SmokeFailure(name + " container is missing from the running Compose project")
        metadata = json.loads(run(["docker", "inspect", container_id], timeout=15).stdout)[0]
        bindings = metadata.get("NetworkSettings", {}).get("Ports", {})
        if name in {"api", "postgres"}:
            target = "8080/tcp" if name == "api" else "5432/tcp"
            published = services[name]["ports"][0]["published"]
            actual = bindings.get(target) or []
            if len(actual) != 1 or actual[0].get("HostIp") != "127.0.0.1" or actual[0].get("HostPort") != str(published):
                raise SmokeFailure(name + " runtime port is not bound only to the configured loopback address")
        elif any(value for value in bindings.values() if value):
            raise SmokeFailure(name + " runtime unexpectedly publishes a host port")


def democtl(*args, timeout=30):
    result = compose("exec", "-T", "api", "/app/democtl", *args, timeout=timeout)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SmokeFailure("democtl returned invalid JSON") from error


def assert_previous_event_retained():
    marker = os.environ.get(
        "COMPOSE_SMOKE_MARKER",
        os.path.join(ROOT, "state", ".compose-smoke-event.json"),
    )
    if not os.path.exists(marker):
        return
    try:
        with open(marker, "r", encoding="utf-8") as stream:
            previous = json.load(stream)
    except (OSError, json.JSONDecodeError):
        raise SmokeFailure("previous Compose smoke marker is unreadable")
    event_id = previous.get("event_id", "")
    if not re.fullmatch(r"[0-9a-f-]{36}", event_id):
        raise SmokeFailure("previous Compose smoke marker has no valid event ID")
    status, body = api_get("/v1/events/" + event_id, authenticated=True)
    if status != 200:
        raise SmokeFailure("previous accepted event did not survive the Compose restart")
    try:
        event = json.loads(body)
    except json.JSONDecodeError as error:
        raise SmokeFailure("retained event response was invalid") from error
    deliveries = event.get("deliveries", [])
    if event.get("id") != event_id or len(deliveries) != 2 or any(item.get("status") != "succeeded" for item in deliveries):
        raise SmokeFailure("retained event or deliveries changed after restart")
    scenario_id = previous.get("scenario_id", "")
    if not re.fullmatch(r"compose_smoke_[0-9a-f]{16}", scenario_id):
        raise SmokeFailure("previous Compose smoke marker has no valid scenario ID")
    outcomes = democtl("scenario", "inspect", "--id", scenario_id, "--receiver", "both")
    if len(outcomes) != 2 or any(item.get("effect_count") != 1 for item in outcomes):
        raise SmokeFailure("previous receiver effects did not survive the Compose restart")


def main():
    marker = os.environ.get(
        "COMPOSE_SMOKE_MARKER",
        os.path.join(ROOT, "state", ".compose-smoke-event.json"),
    )
    worker_stopped = False
    try:
        config, _, api_token = validate_compose_config()

        status, _ = api_get("/health/ready")
        if status != 200:
            raise SmokeFailure("API readiness endpoint is unhealthy")
        status, page = api_get("/")
        if status != 200 or b"root" not in page:
            raise SmokeFailure("API did not serve the built dashboard shell")
        asset_paths = set(re.findall(rb"(?:src|href)=[\"']([^\"']+\.(?:js|css))(?:\?[^\"']*)?[\"']", page))
        if not asset_paths:
            raise SmokeFailure("dashboard shell did not reference a built JS or CSS asset")
        for raw_path in asset_paths:
            asset_path = raw_path.decode("utf-8")
            if not asset_path.startswith("/"):
                asset_path = "/" + asset_path
            asset_status, asset = api_get(asset_path)
            if asset_status != 200 or not asset:
                raise SmokeFailure("built dashboard asset did not load")
            if api_token.encode("utf-8") in asset:
                raise SmokeFailure("API token was embedded in a dashboard asset")
        status, body = api_get("/v1/destinations", authenticated=True)
        if status != 200:
            raise SmokeFailure("authenticated browser API endpoint did not respond successfully")
        destinations = json.loads(body).get("items", [])
        if {item.get("code") for item in destinations} != {"fulfillment", "notification"}:
            raise SmokeFailure("authenticated API returned an unexpected destination set")

        assert_image_is_private()
        assert_runtime_bindings(config)
        no_env = compose("exec", "-T", "api", "test", "!", "-e", "/app/.env", check=False)
        if no_env.returncode != 0:
            raise SmokeFailure("private .env file was copied into the application image")
        user = compose("exec", "-T", "api", "id", "-u").stdout.strip()
        if user != "10001":
            raise SmokeFailure("API process is not running as the non-root image user")

        assert_previous_event_retained()
        scenario = democtl("scenario", "create", "--id", "compose_smoke", "--receiver", "both", "--mode", "normal", "--dedupe", "on")
        scenario_id = scenario.get("scenario_id", "")
        if not scenario_id.startswith("compose_smoke_"):
            raise SmokeFailure("democtl did not create a fresh receiver scenario")

        compose("stop", "--timeout", "1", "worker")
        worker_stopped = True
        accepted = democtl("publish", "--scenario", scenario_id, "--order", "compose-smoke-order", "--destinations", "fulfillment,notification")
        event_id = accepted.get("event_id", "")
        if not re.fullmatch(r"[0-9a-f-]{36}", event_id):
            raise SmokeFailure("democtl did not receive a valid accepted event ID")

        compose("start", "worker")
        worker_stopped = False
        completed = democtl("wait", "--event", event_id, "--state", "succeeded", "--timeout", "60s", timeout=70)
        if completed.get("event_id") != event_id or completed.get("state") != "succeeded":
            raise SmokeFailure("accepted event did not complete after the worker restarted")
        outcomes = democtl("scenario", "inspect", "--id", scenario_id, "--receiver", "both")
        if len(outcomes) != 2 or any(item.get("effect_count") != 1 for item in outcomes):
            raise SmokeFailure("receiver-side committed effect count was not exactly one per destination")
        if {item.get("receiver") for item in outcomes} != {"fulfillment", "notification"}:
            raise SmokeFailure("receiver inspection omitted a destination")

        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, "w", encoding="utf-8") as stream:
            json.dump({"event_id": event_id, "scenario_id": scenario_id}, stream)
            stream.write("\n")
        print("Compose smoke passed: static UI, authenticated API, democtl, worker restart, and two receiver effects.")
        print("Run make down, make up, then make compose-smoke to verify the accepted event survives a normal Compose restart.")
        return 0
    except (SmokeFailure, subprocess.SubprocessError, OSError, ValueError, KeyError) as error:
        if worker_stopped:
            compose("start", "worker", check=False)
        if isinstance(error, SmokeFailure):
            message = str(error)
        else:
            message = "runtime command, network request, or response validation failed"
        print("Compose smoke failed: " + message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
