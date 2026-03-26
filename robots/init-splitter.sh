#!/bin/bash

# Creating minimal Splitter configuration for robots example
# Execute with:
# > docker exec -i robots-splitter-1-1 /bin/bash < ./init-splitter.sh

/usr/local/bin/splitterctl --insecure -e localhost:50051 tenants new facilities
/usr/local/bin/splitterctl --insecure -e localhost:50051 services new facilities/robots region
/usr/local/bin/splitterctl --insecure -e localhost:50051 domains new global facilities/robots/controllers --shards 4
