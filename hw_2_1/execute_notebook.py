"""Execute the full notebook in a fresh kernel and retain outputs on failure."""
from pathlib import Path
import nbformat
from nbclient import NotebookClient

path = Path(__file__).with_name('hw_2_1.ipynb')
notebook = nbformat.read(path, as_version=4)
try:
    NotebookClient(notebook, timeout=600, kernel_name='python3',
                   resources={'metadata': {'path': str(path.parent)}}).execute()
finally:
    nbformat.write(notebook, path)
print('Notebook executed:', path)
