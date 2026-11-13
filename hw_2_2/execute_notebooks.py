import argparse
from pathlib import Path
import nbformat
from nbclient import NotebookClient

parser=argparse.ArgumentParser()
parser.add_argument('--task', type=int, choices=[1,2,3])
args=parser.parse_args()
for number in ([args.task] if args.task else [1,2,3]):
    path=Path(__file__).with_name(f'task{number}.ipynb')
    notebook=nbformat.read(path,as_version=4)
    try:
        NotebookClient(notebook,timeout=7200,kernel_name='python3',resources={'metadata':{'path':str(path.parent)}}).execute()
    finally:
        nbformat.write(notebook,path)
    print('Executed:',path,flush=True)
