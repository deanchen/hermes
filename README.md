Start redis

    redis-server

Run soulmate api server

    soulmate-web --foreground --no-launch --redis=redis://localhost:6379/0

Load data in to soulmate

    node preprocessor.js titles1000.txt | soulmate load paper --redis=redis://localhost:6379/0

Start demo autocomplete field web server

    python -m SimpleHTTPServer &

**Visit http://localhost:8000/demo**
