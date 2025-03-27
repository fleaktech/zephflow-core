#!/usr/bin/env python3
import argparse
import base64
import json
import subprocess
import sys


def read_multiline_input():
    """Read multiline input from stdin until empty line is encountered."""
    lines = []
    while True:
        line = sys.stdin.readline()
        if line == '\n':
            break
        lines.append(line)
    return ''.join(lines)


def encode_base64(text):
    """Convert text to base64 string."""
    return base64.b64encode(text.encode()).decode()


def process_log(jar_path, eval_expression_base64, log_base64):
    """Execute Java command to process the log."""
    command = [
        'java',
        '-jar',
        jar_path,
        '-t',
        'EVAL',
        '-el',
        eval_expression_base64,
        '-i',
        log_base64,
        'log'
    ]

    try:
        result = subprocess.run(
            command,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("============= OCSF =============")
        print(json.dumps(list(json.loads(result.stdout).values())[0], indent=2))
        if result.stderr:
            print("Error:", result.stderr, file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error executing Java command: {e}", file=sys.stderr)
        if e.stderr:
            print(f"Java error output: {e.stderr}", file=sys.stderr)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process logs using Fleak executor')
    parser.add_argument('jar_path', help='Path to the Fleak executor jar file')
    parser.add_argument('eval_file', help='File containing the Fleak eval expression')

    args = parser.parse_args()

    # Read and encode eval expression
    try:
        with open(args.eval_file, 'r') as f:
            eval_expression = f.read()
        eval_expression_base64 = encode_base64(json.dumps([eval_expression]))
    except IOError as e:
        print(f"Error reading eval file: {e}", file=sys.stderr)
        sys.exit(1)

    print("Enter log entries (type or enter to quit):")

    # Main processing loop
    while True:
        # Read log entry
        log_text = read_multiline_input()

        if not log_text:
            break

        # Encode log text and process
        log_base64 = encode_base64(log_text)
        process_log(args.jar_path, eval_expression_base64, log_base64)

        print("\nEnter next log entry (type enter to quit):")


if __name__ == '__main__':
    main()
