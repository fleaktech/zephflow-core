"""Render the queue's JUnit XML at the normal local Gradle HTML report location."""

from html import escape
from pathlib import Path
import sys
# Parses only locally generated JUnit reports from this runner or its fixtures.
import xml.etree.ElementTree as ET  # nosec B405


def render(reports, output):
    # Reads only JUnit reports generated locally by this test run.
    # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
    suites = [ET.parse(path).getroot() for path in sorted(reports.glob('TEST-*.xml'))]  # nosec B314
    totals = {key: sum(int(suite.get(key, '0')) for suite in suites)
              for key in ('tests', 'failures', 'errors', 'skipped')}
    body = ['<!doctype html><html lang="en"><meta charset="utf-8">',
            '<title>Test results</title><style>body{font:16px system-ui;margin:2rem;max-width:90rem}'
            'table{border-collapse:collapse;width:100%}td,th{padding:.4rem;text-align:left;'
            'border-bottom:1px solid #ccc}pre{white-space:pre-wrap}summary{cursor:pointer}</style>',
            '<h1>Test results</h1><p>' + escape(', '.join(f'{value} {key}' for key, value in totals.items())) + '</p>']
    for suite in suites:
        body.append('<details><summary>' + escape(suite.get('name', '')) + ' — '
                    + escape(suite.get('tests', '0')) + ' tests</summary><table>'
                    '<tr><th>Test</th><th>Result</th><th>Seconds</th></tr>')
        for case in suite.findall('testcase'):
            problems = list(case.findall('failure')) + list(case.findall('error'))
            status = 'FAILED' if problems else 'SKIPPED' if case.find('skipped') is not None else 'PASSED'
            body.append('<tr><td>' + escape(case.get('name', '')) + '</td><td>' + status
                        + '</td><td>' + escape(case.get('time', '0')) + '</td></tr>')
            for problem in problems:
                body.append('<tr><td colspan="3"><pre>' + escape(problem.text or problem.get('message', ''))
                            + '</pre></td></tr>')
        body.append('</table>')
        for stream in ('system-out', 'system-err'):
            if suite.findtext(stream):
                body.append('<h3>' + stream + '</h3><pre>' + escape(suite.findtext(stream)) + '</pre>')
        body.append('</details>')
    body.append('</html>')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(body))


if __name__ == '__main__':
    render(Path(sys.argv[1]), Path(sys.argv[2]))
