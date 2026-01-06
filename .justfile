api:
    rye run python main.py --transport streamable-http --port 8321


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
            -- rye run python main.py --transport streamable-http --port 8321 --app-dir datatable_tools/
    else
        rye run python main.py --transport streamable-http --port 8321 --reload --reload-dir datatable_tools/ 
    fi

deploy:
    ./jenkins.sh  \
      -u dev \
      -t 11409eb6c1b33b9e38d385f84034cf4488 \
      -j view/omcp/job/datatable-mcp \
      -e prod \
      -b main
