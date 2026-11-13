"""Распаковка входных данных из inputs с проверкой SHA-256."""
import hashlib
import json
import shutil
import tempfile
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parent


def checksum(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        while chunk:=stream.read(1024*1024):
            digest.update(chunk)
    return digest.hexdigest()


def extract_inputs(destination=None):
    destination=Path(destination) if destination is not None else ROOT/'data'
    destination=destination.resolve()
    manifest=json.loads((ROOT/'inputs/manifest.json').read_text(encoding='utf-8'))
    with tempfile.TemporaryFile() as combined:
        for part in manifest['parts']:
            path=ROOT/'inputs'/part['name']
            assert path.stat().st_size==part['bytes'] and checksum(path)==part['sha256'], path.name
            with path.open('rb') as stream:
                shutil.copyfileobj(stream,combined)
        combined.seek(0)
        with zipfile.ZipFile(combined) as archive:
            assert set(archive.namelist())=={entry['name'] for entry in manifest['files']}
            for entry in manifest['files']:
                target=(destination/entry['name']).resolve()
                assert target.is_relative_to(destination)
                if target.exists() and checksum(target)==entry['sha256']:
                    continue
                target.parent.mkdir(parents=True,exist_ok=True)
                with archive.open(entry['name']) as src,target.open('wb') as dst:
                    shutil.copyfileobj(src,dst)
                assert target.stat().st_size==entry['bytes'] and checksum(target)==entry['sha256'], entry['name']
    return destination


if __name__=='__main__':
    print('Данные распакованы:',extract_inputs())
