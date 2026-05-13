#!/bin/bash
# Railway start script — runs the correct command for each service.
# Railway automatically sets RAILWAY_SERVICE_NAME for every service.
if [ "$RAILWAY_SERVICE_NAME" = "cache-warmer" ]; then
    echo "Starting cache-warmer: python precompute.py"
    python precompute.py
else
    echo "Starting web service: streamlit"
    streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
fi
