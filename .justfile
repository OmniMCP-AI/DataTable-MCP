api:
    rye run python main.py --transport streamable-http --port 8001


dev:
    #!/usr/bin/env bash

    if command -v watchexec >/dev/null 2>&1; then
        watchexec \
            --watch src \
            --ignore tests \
            --ignore benches \
            --exts py \
            --on-busy-update=restart \
            --stop-signal SIGKILL \
            -- rye run python main.py --transport streamable-http --port 8001 --app-dir datatable_tools/
    else
        rye run python main.py --transport streamable-http --port 8001 --reload --reload-dir datatable_tools/ 
    fi
