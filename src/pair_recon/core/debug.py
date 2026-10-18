from importlib import import_module


def attach(traceback):
    try:
        debugger = import_module("ipdb")
    except ImportError:
        debugger = import_module("pdb")
    debugger.post_mortem(traceback)
