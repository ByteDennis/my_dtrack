from .app.cli import main

exit_code = main()
if exit_code:
    raise SystemExit(exit_code)
