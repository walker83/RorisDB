#!/usr/bin/env python3
"""Import Claude Code logs into HarnessDB."""

import json
import sys
from pathlib import Path

def parse_history_jsonl(filepath):
    """Parse history.jsonl and generate SQL INSERT statements."""
    with open(filepath, 'r') as f:
        for line in f:
            data = json.loads(line.strip())

            # Extract pasted text if exists
            pasted_text = ""
            if data.get('pastedContents'):
                # Get first pasted content
                for key, value in data['pastedContents'].items():
                    if value.get('type') == 'text':
                        pasted_text = value.get('content', '').replace("'", "''")
                        break

            # Escape single quotes in display
            display = data.get('display', '').replace("'", "''")
            timestamp = data.get('timestamp', 0)
            project = data.get('project', '').replace("'", "''")
            session_id = data.get('sessionId', '')

            yield f"('{display}', '{pasted_text}', {timestamp}, '{project}', '{session_id}')"

def parse_transcript_jsonl(filepath, session_file):
    """Parse transcript JSONL files and generate SQL INSERT statements."""
    with open(filepath, 'r') as f:
        for line in f:
            data = json.loads(line.strip())

            msg_type = data.get('type', '')
            timestamp_str = data.get('timestamp', '')
            content = data.get('content', '').replace("'", "''")

            yield f"('{msg_type}', '{timestamp_str}', '{content}', '{session_file}')"

def main():
    claude_dir = Path.home() / '.claude'

    # Import history.jsonl
    history_file = claude_dir / 'history.jsonl'

    batch_size = 100
    batch = []

    with open(history_file, 'r') as f:
        for i, line in enumerate(f):
            data = json.loads(line.strip())

            # Extract pasted text if exists
            pasted_text = ""
            if data.get('pastedContents'):
                for key, value in data['pastedContents'].items():
                    if value.get('type') == 'text':
                        pasted_text = value.get('content', '').replace("'", "''")
                        break

            display = data.get('display', '').replace("'", "''")
            timestamp = data.get('timestamp', 0)
            project = data.get('project', '').replace("'", "''")
            session_id = data.get('sessionId', '')

            batch.append(f"('{display}', '{pasted_text}', {timestamp}, '{project}', '{session_id}')")

            if len(batch) >= batch_size:
                print(f"INSERT INTO command_history (display, pasted_text, timestamp, project, session_id) VALUES\n" +
                      ",\n".join(batch) + ";")
                batch = []

    if batch:
        print(f"INSERT INTO command_history (display, pasted_text, timestamp, project, session_id) VALUES\n" +
              ",\n".join(batch) + ";")

    print("-- End of history")

    # Import transcripts
    transcripts_dir = claude_dir / 'transcripts'
    batch = []

    for jsonl_file in sorted(transcripts_dir.glob('*.jsonl')):
        session_file = jsonl_file.name

        with open(jsonl_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())

                    msg_type = data.get('type', '')
                    timestamp_str = data.get('timestamp', '')

                    # Handle content that might be a list or string
                    content_data = data.get('content', '')
                    if isinstance(content_data, list):
                        # Join list elements
                        content = ' '.join(str(x) for x in content_data).replace("'", "''")
                    else:
                        content = str(content_data).replace("'", "''")

                    # Escape backslashes and other special chars
                    content = content.replace('\\', '\\\\').replace('\n', '\\n')

                    batch.append(f"('{msg_type}', '{timestamp_str}', '{content}', '{session_file}')")

                    if len(batch) >= batch_size:
                        print(f"INSERT INTO transcripts (message_type, timestamp_str, content, session_file) VALUES\n" +
                              ",\n".join(batch) + ";")
                        batch = []
                except Exception as e:
                    print(f"-- Error processing line: {e}")
                    continue

    if batch:
        print(f"INSERT INTO transcripts (message_type, timestamp_str, content, session_file) VALUES\n" +
              ",\n".join(batch) + ";")

    print("-- End of transcripts")

if __name__ == '__main__':
    main()