"""Regression tests for queue dispatch, crash recovery and duration ordering."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import textwrap
import time
import signal
import socket
# Launches trusted local test tools with argument lists, without a shell.
import subprocess  # nosec B404
import sys
import unittest
# Parses only locally generated JUnit reports from this runner or its fixtures.
import xml.etree.ElementTree as ET  # nosec B405
from unittest.mock import patch

import test_queue
import test_queue_report


# A subprocess, not a mock socket: abrupt exits exercise the coordinator's real lifecycle.
WORKER = r'''
import os
from pathlib import Path
import socket
import sys
import time
import signal
arguments = sys.argv[sys.argv.index('QueueWorker') + 1:]
_, directory, port, token, identifier = arguments
directory = Path(directory)
with socket.create_connection(('127.0.0.1', int(port))) as connection:
    control = connection.makefile('rw', buffering=1)
    control.write('READY\t' + token + '\t' + identifier + '\n')
    tests = failed = requests = 0
    for command in control:
        if command.strip() == 'STOP': break
        request, name = command.strip().split('\t')
        if name == 'Crash': os._exit(7)
        if name == 'SessionFailure':
            (directory / 'session-errors.log').write_text('IllegalStateException: fixture session failed')
            control.write('SUMMARY\t0\t0\t1\t0\t1\t1\t0\t0\t0\t1\t0\n')
            sys.exit(1)
        if name == 'DisconnectAndIgnoreTerm':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            connection.shutdown(socket.SHUT_RDWR)
            while True: time.sleep(1)
        time.sleep(0.20 if name == 'Slow' else 0.01)
        tests += 1
        failed += name == 'FailedAssertion'
        requests += 1
        control.write('DONE\t%s\t10000000\t%d\n' % (request, tests))
    code = 1 if failed else 0
    values = [tests, failed, 0, 0, 1, 1, 0, 0, requests, code, 0]
    control.write('SUMMARY\t' + '\t'.join(map(str, values)) + '\n')
sys.exit(code)
'''


class QueueTest(unittest.TestCase):
    def test_interrupt_during_discovery_stops_child_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            executable = directory / 'java'
            executable.write_text('#!/usr/bin/env python3\n'
                                  'import os, pathlib, time\n'
                                  'pathlib.Path("discovery-started").write_text(str(os.getpid()))\n'
                                  'time.sleep(60)\n')
            executable.chmod(0o700)
            configuration = {'java': str(executable), 'classpath': '.', 'jvm_arguments': [],
                             'working_directory': temporary, 'candidates': ['Example'],
                             'run_directory': str(directory / 'run'),
                             'reports_directory': str(directory / 'reports')}
            path = directory / 'configuration.json'
            path.write_text(json.dumps(configuration))
            # Runs this repository's coordinator with the configuration created by this test.
            with subprocess.Popen([sys.executable, str(Path(test_queue.__file__).resolve()), str(path)],  # nosec B603
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
                child = None
                try:
                    marker = directory / 'discovery-started'
                    deadline = time.monotonic() + 10
                    while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.02)
                    self.assertTrue(marker.exists())
                    child = int(marker.read_text())
                    process.send_signal(signal.SIGTERM)
                    output, _ = process.communicate(timeout=15)
                    self.assertEqual(1, process.returncode, output.decode())
                    self.assertTrue((directory / 'reports/TEST-TestQueueInfrastructure.xml').is_file())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()
                    if child is not None:
                        try:
                            os.kill(child, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_existing_gc_destination_is_preserved_without_second_log(self):
        configured = '-Xlog:gc*,safepoint:file=/ci/gc-%p.log:time,uptime,level,tags:filecount=0'
        with tempfile.TemporaryDirectory() as temporary:
            configuration = {'java': 'java', 'classpath': '.', 'jvm_arguments': [configured]}
            command = test_queue.java_command(configuration, Path(temporary), ['run'], 0)
            self.assertEqual([configured], [flag for flag in command if flag.startswith('-Xlog:')])

    def test_local_worker_gets_one_default_gc_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            configuration = {'java': 'java', 'classpath': '.', 'jvm_arguments': []}
            command = test_queue.java_command(configuration, Path(temporary), ['run'], 0)
            logging = [flag for flag in command if flag.startswith('-Xlog:')]
            self.assertEqual(1, len(logging))
            self.assertIn(str(Path(temporary) / 'gc.log'), logging[0])

    def run_queue(self, classes, workers):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            executable = directory / 'worker'
            executable.write_text('#!/usr/bin/env python3\n' + textwrap.dedent(WORKER))
            executable.chmod(0o700)
            configuration = {'java': str(executable), 'classpath': '.', 'jvm_arguments': [],
                             'workers': workers, 'working_directory': temporary,
                             'task': ':fixture:test'}
            with contextlib.redirect_stdout(io.StringIO()):
                return test_queue.run_workers(configuration, directory, classes)

    def test_free_worker_takes_next_class_without_waiting_for_slow_worker(self):
        completed, failures, events = self.run_queue(['Slow', 'Fast', 'Next'], 2)
        self.assertFalse(failures)
        self.assertEqual({'Slow', 'Fast', 'Next'}, set(completed))
        assignments = {row['class_name']: row['worker'] for row in events if row['event'] == 'assign'}
        self.assertEqual(assignments['Fast'], assignments['Next'])
        done = [row['class_name'] for row in events if row['event'] == 'done']
        self.assertLess(done.index('Next'), done.index('Slow'))

    def test_crashed_worker_is_replaced_and_remaining_classes_finish(self):
        completed, failures, events = self.run_queue(['Crash', 'AfterCrash'], 1)
        self.assertEqual({'AfterCrash'}, set(completed))
        self.assertEqual(1, len(failures))
        self.assertEqual('Crash', failures[0]['class_name'])
        self.assertEqual(7, failures[0]['exit_code'])
        self.assertEqual(2, sum(row['event'] == 'worker_started' for row in events))

    def test_failed_assertion_does_not_restart_worker_or_stop_other_classes(self):
        completed, failures, events = self.run_queue(['FailedAssertion', 'AfterFailure'], 1)
        self.assertEqual({'FailedAssertion', 'AfterFailure'}, set(completed))
        self.assertFalse(failures)  # Assertion failure is reported by JUnit, not an infrastructure failure.
        self.assertEqual(1, sum(row['event'] == 'worker_started' for row in events))

    def test_session_failure_preserves_cause_when_active_class_has_no_done_message(self):
        completed, failures, _ = self.run_queue(['SessionFailure', 'AfterCrash'], 1)
        self.assertEqual({'AfterCrash'}, set(completed))
        self.assertEqual(1, len(failures))
        self.assertEqual('SessionFailure', failures[0]['class_name'])
        self.assertIn('IllegalStateException: fixture session failed', failures[0]['details'])

    def test_disconnected_worker_stuck_in_shutdown_is_killed_and_replaced(self):
        with patch.object(test_queue, 'WORKER_SHUTDOWN_SECONDS', 0.1):
            completed, failures, events = self.run_queue(['DisconnectAndIgnoreTerm', 'AfterCrash'], 1)
        self.assertEqual({'AfterCrash'}, set(completed))
        self.assertEqual(1, len(failures))
        self.assertEqual(-9, failures[0]['exit_code'])
        self.assertEqual(2, sum(row['event'] == 'worker_started' for row in events))

    def test_spawn_failure_keeps_already_running_workers_alive(self):
        original = test_queue.subprocess.Popen
        calls = []
        def launch(*arguments, **keywords):
            calls.append(True)
            if len(calls) == 2:
                raise OSError('simulated process limit')
            return original(*arguments, **keywords)
        with patch.object(test_queue.subprocess, 'Popen', side_effect=launch):
            completed, failures, _ = self.run_queue(['Slow', 'Fast', 'Last'], 2)
        self.assertEqual({'Slow', 'Fast', 'Last'}, set(completed))
        self.assertEqual(1, len(failures))
        self.assertIn('simulated process limit', failures[0]['message'])

    def test_truncated_report_preserves_other_workers_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            reports = directory / 'reports'
            reports.mkdir()
            for index in range(2):
                (directory / ('worker-' + str(index))).mkdir()
            (directory / 'worker-0/TEST-Broken.xml').write_text('<testsuite')
            (directory / 'worker-1/TEST-Good.xml').write_text(
                '<testsuite name="Good"><testcase unique-id="good"/></testsuite>')
            failures = []
            identifiers, failed, skipped = test_queue.collect_reports(directory, reports, {'Good': 1}, failures)
            self.assertEqual(['good'], identifiers)
            self.assertEqual((0, 0), (failed, skipped))
            self.assertEqual(1, len(failures))
            self.assertTrue((reports / 'TEST-Good.xml').is_file())

    def test_failed_test_details_reach_console_without_logging_successes_or_skips(self):
        for failure_kind in ('failure', 'error'):
            with self.subTest(failure_kind=failure_kind), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                reports = directory / 'reports'
                reports.mkdir()
                worker = directory / 'worker-0'
                worker.mkdir()
                (worker / 'TEST-Example.xml').write_text(
                    '<testsuite name="Example">'
                    '<testcase classname="Example" name="passes" unique-id="passed"/>'
                    '<testcase classname="Example" name="fails" unique-id="failed">'
                    '<' + failure_kind + ' message="expected success">'
                    'AssertionError: expected &lt;success&gt;\n'
                    '    at Example.fails(Example.java:12)\n'
                    'Caused by: IllegalStateException: disconnected\n'
                    '</' + failure_kind + '></testcase>'
                    '<testcase classname="Example" name="skips" unique-id="skipped">'
                    '<skipped/></testcase></testsuite>')
                output = io.StringIO()
                failures = []
                with contextlib.redirect_stdout(output):
                    identifiers, failed, skipped = test_queue.collect_reports(
                        directory, reports, {}, failures)
                console = output.getvalue()
                self.assertEqual(['passed', 'failed', 'skipped'], identifiers)
                self.assertEqual((1, 1), (failed, skipped))
                self.assertFalse(failures)
                self.assertEqual(1, console.count('"event": "test_failed"'))
                self.assertIn('"class_name": "Example"', console)
                self.assertIn('"test_name": "fails"', console)
                self.assertIn('AssertionError: expected <success>\n'
                              '    at Example.fails(Example.java:12)\n'
                              'Caused by: IllegalStateException: disconnected\n', console)
                self.assertNotIn('passes', console)
                self.assertNotIn('skips', console)
                self.assertTrue((reports / 'TEST-Example.xml').is_file())

    def test_unknown_classes_first_then_longest_duration_independent_of_worker_count(self):
        self.assertEqual(['New', 'Long', 'Short'], test_queue.ordered_classes(
            ['Short', 'New', 'Long'], {'Long': 20, 'Short': 10}))

    def test_invalid_durations_does_not_change_test_membership(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'durations.json'
            path.write_text('interrupted write')
            with contextlib.redirect_stdout(io.StringIO()):
                durations = test_queue.load_durations(path)
            self.assertEqual({}, durations)
            self.assertEqual(['A', 'B'], test_queue.ordered_classes(['B', 'A'], durations))

    def test_local_html_report_contains_current_failures_and_escapes_messages(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'TEST-Example.xml').write_text(
                '<testsuite name="Example" tests="1" failures="1"><testcase name="fails">'
                '<failure>&lt;script&gt;must stay text&lt;/script&gt;</failure>'
                '</testcase></testsuite>')
            output = directory / 'index.html'
            output.write_text('outdated report')
            test_queue_report.render(directory, output)
            html = output.read_text()
            self.assertNotIn('outdated report', html)
            self.assertIn('FAILED', html)
            self.assertIn('&lt;script&gt;', html)
            self.assertNotIn('<script>', html)


@unittest.skipUnless(os.environ.get('TEST_QUEUE_CLASSPATH'),
                     'JUnit fixture classpath is supplied by testQueueInfrastructure')
class JavaWorkerTest(unittest.TestCase):
    def configuration(self, directory):
        return {'java': os.environ['TEST_QUEUE_JAVA'],
                'classpath': os.environ['TEST_QUEUE_CLASSPATH'],
                'jvm_arguments': ['-Xmx128m'], 'max_heap': '128m', 'workers': 1,
                'working_directory': str(directory), 'task': ':fixture:test',
                'run_directory': str(directory / 'run'),
                'reports_directory': str(directory / 'reports'),
                'durations_file': str(directory / 'durations.json')}

    def test_aborted_setup_skips_descendants_and_preserves_nested_class_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$AbortedSetup']
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual((2, 2, 0), (result['tests'], result['skipped'], result['failures']))
            # Reads the report produced by the controlled test fixture above.
            # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
            suite = ET.parse(directory / 'reports/TEST-QueueWorkerFixtures$AbortedSetup.xml').getroot()  # nosec B314
            self.assertEqual({'QueueWorkerFixtures$AbortedSetup',
                              'QueueWorkerFixtures$AbortedSetup$NestedTests'},
                             {case.get('classname') for case in suite.findall('testcase')})

    def test_output_reaches_xml_html_and_console(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Passing']
            configuration['show_standard_streams'] = True
            console = io.StringIO()
            with contextlib.redirect_stdout(console):
                self.assertEqual(0, test_queue.execute(configuration))
            # Reads the report produced by the controlled test fixture above.
            # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
            suite = ET.parse(directory / 'reports/TEST-QueueWorkerFixtures$Passing.xml').getroot()  # nosec B314
            output = directory / 'index.html'
            test_queue_report.render(directory / 'reports', output)
            for stream, message in [('system-out', 'fixture standard output <text>'),
                                    ('system-err', 'fixture standard error <text>')]:
                self.assertIn(message, suite.findtext(stream))
                self.assertIn(message, console.getvalue())
                self.assertIn(message.replace('<text>', '&lt;text&gt;'), output.read_text())

    def test_cached_logger_stream_is_preserved_across_classes_in_one_jvm(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$LoggingFirst',
                                            'QueueWorkerFixtures$LoggingSecond']
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            for name, prefix in [('LoggingFirst', 'first'), ('LoggingSecond', 'second')]:
                # Reads the report produced by the controlled test fixture above.
                # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
                suite = ET.parse(directory / ('reports/TEST-QueueWorkerFixtures$' + name + '.xml')).getroot()  # nosec B314
                self.assertIn(prefix + ' class standard output', suite.findtext('system-out'))
                self.assertIn(prefix + ' class standard error', suite.findtext('system-err'))

    def test_tags_nested_dynamic_parameterized_and_disabled_tests_keep_their_membership(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Mixed', 'QueueWorkerFixtures$Golden']
            configuration['filters'] = {'excludeTags': ['golden']}
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual((1, 9, 1),
                             (result['completed_classes'], result['tests'], result['skipped']))
            identifiers = (directory / 'run/identifiers.txt').read_text()
            self.assertNotIn('goldenMethod', identifiers)
            self.assertNotIn('nestedGolden', identifiers)
            self.assertIn('dynamic-test', identifiers)
            self.assertIn('test-template-invocation', identifiers)
            self.assertEqual(['QueueWorkerFixtures$Mixed'],
                             json.loads((directory / 'run/order.json').read_text()))

    def test_included_tags_apply_to_discovery_and_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Mixed', 'QueueWorkerFixtures$Golden',
                                            'QueueWorkerFixtures$Passing']
            configuration['filters'] = {'includeTags': ['golden']}
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual((2, 3), (result['completed_classes'], result['tests']))

    def test_all_classes_excluded_by_tags_produce_an_empty_successful_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Golden']
            configuration['filters'] = {'excludeTags': ['golden']}
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual((0, 0), (result['completed_classes'], result['tests']))

    def test_methods_remain_sequential_despite_junit_parallel_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Sequential']
            configuration['jvm_arguments'] += [
                '-Djunit.jupiter.execution.parallel.enabled=true',
                '-Djunit.jupiter.execution.parallel.mode.default=concurrent',
                '-Djunit.jupiter.execution.parallel.config.strategy=fixed',
                '-Djunit.jupiter.execution.parallel.config.fixed.parallelism=4']
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, test_queue.execute(configuration))
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual((8, 0), (result['tests'], result['failures']))

    def test_worker_counts_coverage_and_local_duration_history(self):
        for workers in (1, 2):
            with self.subTest(workers=workers), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                configuration = self.configuration(directory)
                configuration['workers'] = workers
                configuration['candidates'] = ['QueueWorkerFixtures$Passing', 'QueueWorkerFixtures$Mixed']
                configuration['jacoco_argument'] = os.environ['TEST_QUEUE_JACOCO_ARGUMENT']
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(0, test_queue.execute(configuration))
                events = [json.loads(line) for line in (directory / 'run/timeline.jsonl').read_text().splitlines()]
                self.assertEqual(workers, sum(row['event'] == 'worker_started' for row in events))
                recordings = list((directory / 'run').glob('worker-*/jacoco.exec'))
                self.assertEqual(workers, len(recordings))
                self.assertTrue(all(recording.stat().st_size > 10 for recording in recordings))
                history = test_queue.load_durations(directory / 'durations.json')
                self.assertEqual(set(configuration['candidates']), set(history))
                configuration['run_directory'] = str(directory / 'second-run')
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(0, test_queue.execute(configuration))
                self.assertEqual(test_queue.ordered_classes(configuration['candidates'], history),
                                 json.loads((directory / 'second-run/order.json').read_text()))

    def test_interrupt_keeps_completed_reports_and_stops_worker(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$Passing', 'QueueWorkerFixtures$Slow']
            path = directory / 'configuration.json'
            path.write_text(json.dumps(configuration))
            # Runs this repository's coordinator with the configuration created by this test.
            with subprocess.Popen([sys.executable, str(Path(test_queue.__file__).resolve()), str(path)],  # nosec B603
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
                worker = None
                try:
                    deadline = time.monotonic() + 20
                    marker = directory / 'slow-started'
                    while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.02)
                    self.assertTrue(marker.exists())
                    worker = int(marker.read_text())
                    process.send_signal(signal.SIGTERM)
                    output, _ = process.communicate(timeout=15)
                    self.assertEqual(1, process.returncode, output.decode())
                    passing = directory / 'reports/TEST-QueueWorkerFixtures$Passing.xml'
                    # Reads the report produced by the controlled test fixture above.
                    # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
                    self.assertEqual('1', ET.parse(passing).getroot().get('tests'))  # nosec B314
                    self.assertTrue((directory / 'reports/TEST-TestQueueInfrastructure.xml').is_file())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(worker, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()
                    if worker is not None:
                        try:
                            os.kill(worker, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_lifecycle_errors_retain_class_and_stack_trace_and_suite_continues(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            configuration['candidates'] = ['QueueWorkerFixtures$' + name
                                            for name in ('FailingSetup', 'FailingCleanup', 'Passing')]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = test_queue.execute(configuration)
            self.assertEqual(1, exit_code)
            result = json.loads((directory / 'run/result.json').read_text())
            self.assertEqual([], result['infrastructure_failures'])
            self.assertEqual(2, result['failures'])
            self.assertEqual(3, result['completed_classes'])
            for name, message in [('FailingSetup', 'fixture setup failed'),
                                  ('FailingCleanup', 'fixture cleanup failed')]:
                owner = 'QueueWorkerFixtures$' + name
                # Reads the report produced by the controlled test fixture above.
                # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
                suite = ET.parse(directory / ('reports/TEST-' + owner + '.xml')).getroot()  # nosec B314
                errors = suite.findall('testcase/error')
                self.assertEqual(1, len(errors))
                self.assertEqual('1', suite.get('errors'))
                self.assertIn(message, errors[0].text)
                self.assertIn(owner, errors[0].text)
                self.assertIn(message, output.getvalue())
                self.assertIn(owner, output.getvalue())
            # Reads the report produced by the controlled test fixture above.
            # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
            setup = ET.parse(directory / 'reports/TEST-QueueWorkerFixtures$FailingSetup.xml').getroot()  # nosec B314
            self.assertEqual(2, len(setup.findall('testcase/skipped')))
            # Reads the report produced by the controlled test fixture above.
            # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
            passing = ET.parse(directory / 'reports/TEST-QueueWorkerFixtures$Passing.xml').getroot()  # nosec B314
            self.assertEqual('0', passing.get('errors'))
            self.assertEqual(1, len(passing.findall('testcase')))

    def test_unsafe_class_names_are_rejected_before_accessing_files(self):
        for variant in ('../outside/Target', 'absolute', 'nested/Target',
                        r'nested\Target', 'Invalid..Name'):
            with self.subTest(name=variant), tempfile.TemporaryDirectory() as temporary, socket.socket() as server:
                root = Path(temporary)
                directory = root / 'worker'
                directory.mkdir()
                (directory / 'nested').mkdir()
                outside = root / 'outside'
                outside.mkdir()
                for filename in ('Target.stdout', 'Target.stderr', 'TEST-Target.xml', 'TEST-Target.xml.tmp'):
                    (outside / filename).write_text('must not be changed')
                original = {path.name: path.read_bytes() for path in outside.iterdir()}
                name = str(outside / 'Target') if variant == 'absolute' else variant
                configuration = self.configuration(directory)
                server.bind(('127.0.0.1', 0))
                server.listen(1)
                server.settimeout(10)
                command = test_queue.java_command(configuration, directory,
                    ['run', str(directory), str(server.getsockname()[1]), 'fixture-token', '0'], 0)
                # Runs the local fixture JVM with trusted test configuration and no shell.
                # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
                with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:  # nosec B603
                    try:
                        connection, _ = server.accept()
                        with connection, connection.makefile('rw', buffering=1) as control:
                            connection.settimeout(10)
                            self.assertTrue(control.readline().startswith('READY\t'))
                            control.write('0\t' + name + '\nSTOP\n')
                            self.assertTrue(control.readline().startswith('SUMMARY\t'))
                        output, _ = process.communicate(timeout=10)
                        self.assertEqual(1, process.returncode, output.decode())
                        self.assertEqual(original, {path.name: path.read_bytes() for path in outside.iterdir()})
                        self.assertEqual([], list(directory.rglob('*.stdout')))
                        self.assertEqual([], list(directory.rglob('*.stderr')))
                        self.assertEqual([], list(directory.rglob('*.xml*')))
                        self.assertIn('IllegalArgumentException: Invalid test class name',
                                      (directory / 'session-errors.log').read_text())
                    finally:
                        if process.poll() is None:
                            process.kill()
                            process.communicate()

    def test_session_close_does_not_rewrite_completed_class_report(self):
        with tempfile.TemporaryDirectory() as temporary, socket.socket() as server:
            directory = Path(temporary)
            configuration = self.configuration(directory)
            server.bind(('127.0.0.1', 0))
            server.listen(1)
            server.settimeout(10)
            command = test_queue.java_command(configuration, directory,
                ['run', str(directory), str(server.getsockname()[1]), 'fixture-token', '0'], 0)
            # Runs the local fixture JVM with trusted test configuration and no shell.
            # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:  # nosec B603
                try:
                    connection, _ = server.accept()
                    with connection, connection.makefile('rw', buffering=1) as control:
                        connection.settimeout(10)
                        self.assertTrue(control.readline().startswith('READY\t'))
                        control.write('0\tQueueWorkerFixtures$Passing\n')
                        self.assertTrue(control.readline().startswith('DONE\t'))
                        report = directory / 'TEST-QueueWorkerFixtures$Passing.xml'
                        modification = report.stat().st_mtime_ns
                        control.write('STOP\n')
                        self.assertTrue(control.readline().startswith('SUMMARY\t'))
                    process.communicate(timeout=10)
                    self.assertEqual(0, process.returncode)
                    self.assertEqual(modification, report.stat().st_mtime_ns)
                    self.assertFalse((directory / 'identifiers.txt').exists())
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()


if __name__ == '__main__':
    unittest.main()
