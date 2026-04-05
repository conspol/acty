# Manual TUI Console Bindings Check

This is a real-terminal (non-`run_test`) check for console binding visibility and behavior.

## Run (console capture disabled)

```bash
python examples/tui_console_bindings_manual_check.py --no-console
```

Expected:
- Footer **does not** show console bindings `P`, `F`, `C`.
- Only scheduler log pane is visible.
- Press `p` to pause scheduler logs: the scheduler log pane should stop updating.
- Press `p` again to resume scheduler logs.
- Press `q` to quit.

## Run (console capture enabled)

```bash
python examples/tui_console_bindings_manual_check.py --console
```

Expected:
- Footer **does** show console bindings `P`, `F`, `C`.
- Console log pane shows lines like `console print N` and `console log N`.
- Press `P` to pause console logs: console log pane should stop updating; resume with `P` again.
- Press `p` to pause scheduler logs: scheduler log pane should stop updating (console logs can still continue).
- Press `p` again to resume scheduler logs.
- Press `q` to quit.

## Notes

- Scheduler bindings (`p`, `f`, `c`) should continue to work with console capture enabled.
- This check is manual and intended for a real terminal TTY session.
