import sys

try:
    from .cli import run
except ModuleNotFoundError as exc:
    if exc.name == "typer":
        print("[ERROR] Missing dependency: typer", file=sys.stderr)
        print("Install dependencies: pip install -r requirements.txt", file=sys.stderr)
        raise SystemExit(1)
    raise


if __name__ == "__main__":
    run()
