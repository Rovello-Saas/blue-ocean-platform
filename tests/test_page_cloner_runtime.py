import pytest

import src.page_cloner.runtime as runtime


@pytest.fixture(autouse=True)
def reset_runtime_state():
    runtime._PROCESS = None
    runtime._PROCESS_LOG = None
    runtime._INSTALL_DONE = False
    runtime._CHROME_INSTALL_DONE = False
    yield
    if runtime._PROCESS_LOG:
        runtime._PROCESS_LOG.close()
    runtime._PROCESS = None
    runtime._PROCESS_LOG = None
    runtime._INSTALL_DONE = False
    runtime._CHROME_INSTALL_DONE = False


def test_running_local_cloner_is_reused_before_setup(monkeypatch, tmp_path):
    (tmp_path / "internal-page-cloner").mkdir()
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runtime, "PAGE_CLONER_URL", "http://127.0.0.1:3333")
    monkeypatch.setattr(runtime, "_health_ok", lambda base_url: True)

    setup_calls = []
    monkeypatch.setattr(
        runtime,
        "_ensure_node_modules",
        lambda cloner_dir: setup_calls.append("npm"),
    )
    monkeypatch.setattr(
        runtime,
        "_ensure_chrome",
        lambda cloner_dir, env: setup_calls.append("chrome"),
    )

    assert runtime.ensure_internal_page_cloner() == "http://127.0.0.1:3333"
    assert setup_calls == []


def test_stale_install_marker_does_not_skip_dependency_install(monkeypatch, tmp_path):
    (tmp_path / ".install-complete").write_text("ok\n")
    (tmp_path / "package-lock.json").write_text("{}\n")
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runtime.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(runtime, "_move_aside_node_modules", lambda path: None)

    commands = []

    def fake_run_setup_command(command, *, cwd, env, timeout, failure_message):
        commands.append((command, cwd, env, timeout, failure_message))

    monkeypatch.setattr(runtime, "_run_setup_command", fake_run_setup_command)

    runtime._ensure_node_modules(tmp_path)

    assert commands
    assert commands[0][0] == ["npm", "ci", "--omit=dev", "--no-audit", "--no-fund"]
    assert commands[0][1] == tmp_path
    assert float((tmp_path / ".install-complete").read_text()) >= 0


def test_enotempty_install_error_cleans_and_retries(monkeypatch, tmp_path):
    (tmp_path / "package-lock.json").write_text("{}\n")
    monkeypatch.setattr(runtime, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(runtime.shutil, "which", lambda binary: f"/usr/bin/{binary}")

    cleanups = []
    monkeypatch.setattr(
        runtime,
        "_move_aside_node_modules",
        lambda path: cleanups.append(path.name),
    )

    commands = []

    def fake_run_setup_command(command, *, cwd, env, timeout, failure_message):
        commands.append(command)
        if len(commands) == 1:
            raise RuntimeError("npm ERR! code ENOTEMPTY")

    monkeypatch.setattr(runtime, "_run_setup_command", fake_run_setup_command)

    runtime._ensure_node_modules(tmp_path)

    assert len(commands) == 2
    assert cleanups == ["node_modules", "node_modules"]
    assert (tmp_path / ".install-complete").exists()


def test_stale_chrome_marker_does_not_skip_browser_install(monkeypatch, tmp_path):
    (tmp_path / ".chrome-install-complete").write_text("ok\n")
    monkeypatch.setattr(runtime.shutil, "which", lambda binary: f"/usr/bin/{binary}")
    monkeypatch.setattr(runtime, "_find_system_chrome", lambda: None)

    commands = []

    def fake_run_setup_command(command, *, cwd, env, timeout, failure_message):
        commands.append((command, cwd, env, timeout, failure_message))

    monkeypatch.setattr(runtime, "_run_setup_command", fake_run_setup_command)

    runtime._ensure_chrome(
        tmp_path,
        {"PUPPETEER_CACHE_DIR": str(tmp_path / "missing-puppeteer-cache")},
    )

    assert commands
    assert commands[0][0] == ["npx", "puppeteer", "browsers", "install", "chrome"]
    assert commands[0][1] == tmp_path
    assert (tmp_path / ".chrome-install-complete").read_text() == "ok\n"


def test_system_chrome_skips_browser_install(monkeypatch, tmp_path):
    browser = tmp_path / "Google Chrome"
    browser.write_text("")
    env = {"PUPPETEER_CACHE_DIR": str(tmp_path / "missing-puppeteer-cache")}
    monkeypatch.setattr(runtime, "_find_system_chrome", lambda: browser)

    commands = []
    monkeypatch.setattr(
        runtime,
        "_run_setup_command",
        lambda command, *, cwd, env, timeout, failure_message: commands.append(command),
    )

    runtime._ensure_chrome(tmp_path, env)

    assert commands == []
    assert env["PUPPETEER_EXECUTABLE_PATH"] == str(browser)
    assert (tmp_path / ".chrome-install-complete").read_text() == f"system:{browser}\n"


def test_browser_install_cache_error_cleans_and_retries(monkeypatch, tmp_path):
    cache_dir = tmp_path / "puppeteer-cache"
    cache_dir.mkdir()
    (cache_dir / "partial.zip").write_text("bad zip")
    monkeypatch.setattr(runtime, "_find_system_chrome", lambda: None)
    monkeypatch.setattr(runtime.shutil, "which", lambda binary: f"/usr/bin/{binary}")

    commands = []
    cleanups = []
    monkeypatch.setattr(runtime, "_safe_remove_tree", lambda path: cleanups.append(path))

    def fake_run_setup_command(command, *, cwd, env, timeout, failure_message):
        commands.append(command)
        if len(commands) == 1:
            raise RuntimeError("DefaultProvider: end of central directory record signature not found")

    monkeypatch.setattr(runtime, "_run_setup_command", fake_run_setup_command)

    runtime._ensure_chrome(tmp_path, {"PUPPETEER_CACHE_DIR": str(cache_dir)})

    assert commands == [
        ["npx", "puppeteer", "browsers", "install", "chrome"],
        ["npx", "puppeteer", "browsers", "install", "chrome@stable"],
    ]
    assert cleanups == [cache_dir]
    assert (tmp_path / ".chrome-install-complete").read_text() == "ok\n"
