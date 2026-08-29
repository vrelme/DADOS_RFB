import logging
import zipfile
from pathlib import Path

from app.etl.orchestrator import prepare_zip_inputs


def test_prepare_zip_inputs_extracts_archives_to_staging_and_final_dir(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()

    payload_dir = tmp_path / "payload"
    payload_dir.mkdir()
    (payload_dir / "arquivo.csv").write_text("id,nome\n1,teste\n", encoding="utf-8")

    nested_zip_path = payload_dir / "nested.zip"
    with zipfile.ZipFile(nested_zip_path, "w") as archive:
        archive.write(payload_dir / "arquivo.csv", arcname="arquivo.csv")

    root_zip_path = input_dir / "dados.zip"
    with zipfile.ZipFile(root_zip_path, "w") as archive:
        archive.write(nested_zip_path, arcname="nested.zip")

    logger = logging.getLogger("test_zip_preprocessing")
    prepared_files = prepare_zip_inputs(input_dir, extract_dir, logger)

    staging_dir = input_dir / "arquivos"
    assert (staging_dir / "dados" / "nested.zip").exists()
    assert (extract_dir / "arquivo.csv").exists()
    assert any(path.name == "arquivo.csv" for path in prepared_files)
