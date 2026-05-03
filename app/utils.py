from pathlib import Path

def create_folders():
    folders = [
        Path("logs"),
        Path("data/input"),
        Path("data/extracted"),
        Path("docs"),
        Path("tests")
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)