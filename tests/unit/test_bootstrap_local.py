from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from naval.bootstrap import LOCAL_COMPOSE_FILE, LOCAL_ENV_FILE, bootstrap_local, ensure_local_bootstrap_files


def main() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        compose, env_file, created = ensure_local_bootstrap_files(root)
        assert compose.name == LOCAL_COMPOSE_FILE
        assert env_file.name == LOCAL_ENV_FILE
        assert compose.exists()
        assert env_file.exists()
        assert len(created) == 2
        text = compose.read_text(encoding="utf-8")
        assert "temporal" in text.lower()
        assert "localstack" in text.lower()

        # second run should be idempotent.
        _, _, created2 = ensure_local_bootstrap_files(root)
        assert created2 == []

        result = bootstrap_local(root, apply=False)
        assert result.started is False
        assert "scaffold generated" in result.message

    print("PASS: bootstrap_local")


if __name__ == "__main__":
    main()
