# Getting Started with Streaming using PySpark

## Virtual Environment Setup

The usual:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the demo

### Cleanup previous runs

Make sure that all following directories exists and are empty:
- `./data/checkpoint`
- `./data/lz`
- `./data/output`

### Prepare upstream Data
In `./data/upstream` place the data you will stream through the Landing Zone (LZ).
The format of each JSON should be:

```json
{"name": "Frodo","age": 33}
{"name": "Sam","age": 38}
{"name": "Gandalf","age": 2019}
```

- One JSON object per line
- No empty line at the end


### Start the service

Activate the virtual environment and run:

```bash
spark-submit streaming_app.py
```

**⚠️ IMPORTANT:** The application terminates after ideling for `timeout_seconds` (i.e., no new files added to `data/lz`).

### Simulate data stream

While the application is running move files from the `data/upstream` to `data/lz`.
After moving one or more files, head to the [monitor notebook](./monitor.ipynb) and run the last cell to see the updated table.
You can move some files to LZ while the application is down and then restart it, to test how it picks up new data.
