import sys

import nbformat


# >>> git clean filter: a notebook on stdin, the same notebook without outputs on stdout <<< #
def main() -> None:
    notebook = nbformat.read(sys.stdin, as_version=4)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None
    cell_metadata = ("execution", "collapsed", "scrolled")
    for cell in notebook.cells:
        for key in cell_metadata:
            cell.metadata.pop(key, None)
    nbformat.write(notebook, sys.stdout)


if __name__ == "__main__":
    main()
