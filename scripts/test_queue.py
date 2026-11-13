"""Run the Gradle-selected JUnit classes through a shared queue of persistent JVMs."""

import argparse
import collections
import gzip
import json
import os
from pathlib import Path
import secrets
import selectors
import shutil
import signal
import socket
# Launches trusted local test tools with argument lists, without a shell.
import subprocess  # nosec B404
import sys
import time
# Parses only locally generated JUnit reports from this runner or its fixtures.
import xml.etree.ElementTree as ET  # nosec B405

WORKER_SHUTDOWN_SECONDS = 10


class QueueInterrupted(Exception):
    """Cancellation must not be swallowed as an interrupted selector system call."""


def emit(event, **values):
    print('TEST_QUEUE ' + json.dumps({'event': event, **values}), flush=True)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def ordered_classes(classes, durations):
    return sorted(classes, key=lambda name: (name in durations, -durations.get(name, 0), name))


def load_durations(path):
    if not path.is_file():
        return {}
    try:
        values = json.loads(path.read_text())['durations_seconds']
        return {name: float(duration) for name, duration in values.items()
                if isinstance(duration, (int, float)) and 0 <= duration < float('inf')}
    except (ValueError, KeyError, TypeError):
        emit('invalid_durations', path=str(path))
        return {}


def java_command(configuration, directory, arguments, worker=None):
    flags = list(configuration['jvm_arguments'])
    for name, values in configuration.get('filters', {}).items():
        if values:
            flags.append('-Dtest.queue.' + name + '=' + '\n'.join(values))
    # CI already supplies its GC destination. Only add the local default when absent.
    if not any(flag.startswith('-Xlog:') and 'gc' in flag.split(':')[1] for flag in flags):
        flags += ['-Xlog:gc*:file=' + str(directory / 'gc.log')
                  + ':time,uptime,level,tags:filecount=0']
    if worker is not None:
        flags += ['-Dorg.gradle.test.worker=queue-' + str(worker),
                  '-Dci.queue.worker=' + str(worker)]
        if configuration.get('jacoco_argument'):
            agent, options = configuration['jacoco_argument'].split('=', 1)
            options = [value for value in options.split(',')
                       if not value.startswith(('destfile=', 'append='))]
            options += ['destfile=' + str(directory / 'jacoco.exec'), 'append=false']
            flags.append(agent + '=' + ','.join(options))
    # Keep the large runtime classpath below Linux's per-argument size limit.
    classpath_file = directory / 'classpath.args'
    classpath = configuration['classpath'].replace('\\', '\\\\').replace('"', '\\"')
    classpath_file.write_text('-cp\n"' + classpath + '"\n')
    return [configuration['java']] + flags + ['@' + str(classpath_file), 'QueueWorker'] + arguments


def run_workers(configuration, directory, classes):
    pending = collections.deque(enumerate(classes))
    workers = {}
    completed = {}
    failures = []
    events = []
    selector = selectors.DefaultSelector()
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(configuration['workers'])
    server.setblocking(False)
    selector.register(server, selectors.EVENT_READ, None)
    token = secrets.token_hex(16)
    connections = {}
    buffers = {}
    started = time.monotonic()
    restarts = 0
    next_worker = 0
    # Same bounded restart policy as xdist: replace crashed processes, continue the suite.
    maximum_restarts = 4 * configuration['workers']

    def record(event, **values):
        row = {'event': event, 'seconds': time.monotonic() - started, **values}
        events.append(row)
        with (directory / 'timeline.jsonl').open('a') as output:
            output.write(json.dumps(row) + '\n')

    def launch():
        nonlocal next_worker
        identifier = next_worker
        next_worker += 1
        output_directory = directory / ('worker-' + str(identifier))
        output = None
        try:
            output_directory.mkdir()
            command = java_command(configuration, output_directory,
                                   ['run', str(output_directory), str(server.getsockname()[1]),
                                    token, str(identifier)], identifier)
            output = (output_directory / 'output.log').open('wb')
            # Java and arguments come from trusted Gradle configuration; no shell is used.
            # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
            process = subprocess.Popen(command, cwd=configuration['working_directory'],  # nosec B603
                                       env={**os.environ, **configuration.get('environment', {})},
                                       stdout=output, stderr=subprocess.STDOUT)
        except OSError as error:
            if output is not None:
                output.close()
            failure = {'worker': identifier, 'message': 'Cannot start worker: ' + str(error)}
            failures.append(failure)
            record('worker_start_failed', **failure)
            emit('worker_start_failed', **failure)
            return
        workers[identifier] = {'process': process, 'output': output, 'directory': output_directory,
                               'active': None, 'finished': False, 'summary': None,
                               'connection': None, 'problem': None, 'tests': 0}
        record('worker_started', worker=identifier, pid=process.pid)

    def stop_broken_worker(worker, message):
        if not worker['problem']:
            worker['problem'] = message
            worker['shutdown_started'] = time.monotonic()
            if worker['process'].poll() is None:
                worker['process'].terminate()

    def assign(identifier):
        worker = workers[identifier]
        if pending:
            request, name = pending.popleft()
            worker['active'] = (request, name, time.time_ns() // 1_000_000)
            record('assign', worker=identifier, request=request, class_name=name)
            command = str(request) + '\t' + name
        else:
            command = 'STOP'
        try:
            worker['connection'].sendall((command + '\n').encode())
        except OSError as error:
            stop_broken_worker(worker, 'Control connection failed: ' + str(error))

    def disconnect(connection):
        selector.unregister(connection)
        connections.pop(connection, None)
        buffers.pop(connection, None)
        connection.close()

    def interrupted(number, frame):
        raise QueueInterrupted('Test queue interrupted by signal ' + str(number))

    handlers = {number: signal.signal(number, interrupted)
                for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        for _ in range(min(configuration['workers'], len(classes))):
            launch()
        while any(not worker['finished'] for worker in workers.values()):
            for key, _ in selector.select(0.2):
                if key.fileobj is server:
                    connection, _ = server.accept()
                    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    selector.register(connection, selectors.EVENT_READ)
                    connections[connection] = None
                    buffers[connection] = b''
                    continue
                connection = key.fileobj
                try:
                    block = connection.recv(65536)
                except ConnectionResetError:
                    block = b''
                if not block:
                    identifier = connections[connection]
                    disconnect(connection)
                    if identifier is not None:
                        worker = workers[identifier]
                        if worker['summary'] is None and worker['process'].poll() is None:
                            stop_broken_worker(worker, 'Worker disconnected without a summary')
                    continue
                buffers[connection] += block
                while b'\n' in buffers[connection]:
                    line, buffers[connection] = buffers[connection].split(b'\n', 1)
                    parts = line.decode().split('\t')
                    if parts[0] == 'READY':
                        require(parts[1] == token, 'Invalid worker token')
                        identifier = int(parts[2])
                        worker = workers[identifier]
                        require(worker['connection'] is None, 'Duplicate worker connection')
                        worker['connection'] = connection
                        connections[connection] = identifier
                        assign(identifier)
                    elif parts[0] == 'DONE':
                        identifier = connections[connection]
                        worker = workers[identifier]
                        require(worker['active'] is not None, 'Unexpected completion')
                        request, name, epoch = worker['active']
                        require(int(parts[1]) == request and name not in completed,
                                'Duplicate or mismatched class completion')
                        completed[name] = int(parts[2]) / 1e9
                        tests = int(parts[3]) - worker['tests']
                        worker['tests'] = int(parts[3])
                        worker['active'] = None
                        record('done', worker=identifier, class_name=name, request=request,
                               duration_ms=completed[name] * 1000, tests=tests,
                               task=configuration['task'], start_epoch_ms=epoch,
                               end_epoch_ms=time.time_ns() // 1_000_000)
                        if len(completed) % 25 == 0 or len(completed) == len(classes):
                            emit('progress', classes=len(completed), total_classes=len(classes),
                                 tests=sum(item['tests'] for item in workers.values()),
                                 seconds=round(time.monotonic() - started, 2))
                        assign(identifier)
                    elif parts[0] == 'SUMMARY':
                        worker = workers[connections[connection]]
                        fields = ['tests', 'failed', 'session_failures', 'duplicates', 'opened',
                                  'closed', 'discovery_ns', 'execution_ns', 'requests', 'exit', 'skipped']
                        require(len(parts) == len(fields) + 1, 'Invalid worker summary')
                        worker['summary'] = dict(zip(fields, map(int, parts[1:])))
                        worker['summary_time'] = time.monotonic()
                    else:
                        raise RuntimeError('Unexpected worker control message')
            for identifier, worker in list(workers.items()):
                if worker['finished']:
                    continue
                process = worker['process']
                if (worker['summary'] is not None and process.poll() is None
                        and time.monotonic() - worker['summary_time'] > 15):
                    stop_broken_worker(worker, 'Worker did not exit after completing its session')
                if (worker['problem'] and process.poll() is None
                        and time.monotonic() - worker['shutdown_started'] > WORKER_SHUTDOWN_SECONDS):
                    process.kill()
                if process.poll() is None:
                    continue
                # Drain a summary already in the socket before treating an exit as a crash.
                if worker['connection'] in connections:
                    continue
                worker['finished'] = True
                worker['output'].close()
                summary = worker['summary']
                crashed = summary is None or worker['active'] is not None or worker['problem']
                details_file = worker['directory'] / 'session-errors.log'
                details = details_file.read_text() if details_file.is_file() else ''
                if crashed:
                    name = worker['active'][1] if worker['active'] else None
                    failure = {'worker': identifier, 'class_name': name, 'exit_code': process.returncode,
                               'message': worker['problem'] or 'Worker exited before completing its work',
                               'details': details}
                    failures.append(failure)
                    record('worker_crashed', **failure)
                    emit('worker_crashed', **failure)
                    if pending and restarts < maximum_restarts:
                        restarts += 1
                        launch()
                elif (process.returncode != summary['exit'] or summary['session_failures']
                      or summary['duplicates'] or summary['opened'] != 1 or summary['closed'] != 1):
                    failure = {'worker': identifier, 'message': 'Worker session/report failure',
                               'summary': summary, 'exit_code': process.returncode,
                               'details': details}
                    failures.append(failure)
                    emit('worker_session_failed', **failure)
        if pending:
            failures.append({'message': 'No workers remaining after the restart limit',
                             'unexecuted_classes': [name for _, name in pending]})
    except (QueueInterrupted, KeyboardInterrupt) as error:
        failures.append({'message': str(error) or 'Test queue interrupted'})
        record('interrupted')
    finally:
        for worker in workers.values():
            if worker['process'].poll() is None:
                worker['process'].terminate()
        for worker in workers.values():
            try:
                worker['process'].wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker['process'].kill()
                worker['process'].wait()
            worker['output'].close()
        for connection in list(connections):
            disconnect(connection)
        selector.close()
        server.close()
        for number, handler in handlers.items():
            signal.signal(number, handler)
    return completed, failures, events


def write_infrastructure_failure(directory, failures):
    suite = ET.Element('testsuite', name='TestQueueInfrastructure', tests='1', errors='1',
                       failures='0', skipped='0', time='0')
    case = ET.SubElement(suite, 'testcase', classname='TestQueueInfrastructure',
                         name='all selected classes and worker sessions must finish', time='0')
    ET.SubElement(case, 'error', message='Test queue did not complete successfully').text = json.dumps(failures, indent=2)
    ET.ElementTree(suite).write(directory / 'TEST-TestQueueInfrastructure.xml',
                               encoding='utf-8', xml_declaration=True)


def collect_reports(directory, reports, completed, failures, show_standard_streams=False):
    identifiers = []
    failed_tests = skipped = 0
    for report in sorted(directory.glob('worker-*/TEST-*.xml')):
        try:
            # Reads only JUnit reports generated locally by this test run.
            # nosemgrep: python.lang.security.use-defused-xml-parse.use-defused-xml-parse
            suite = ET.parse(report).getroot()  # nosec B314
        except (ET.ParseError, OSError) as error:
            failures.append({'message': 'Cannot read JUnit report: ' + str(error),
                             'report': str(report)})
            continue
        owner = suite.attrib['name']
        if (reports / report.name).exists():
            failures.append({'message': 'Duplicate class report: ' + owner})
            continue
        if owner in completed:
            suite.set('time', str(completed[owner]))
        if show_standard_streams:
            for stream in ('system-out', 'system-err'):
                output = suite.findtext(stream)
                if output:
                    print(output, end='', flush=True)
        for case in suite.findall('testcase'):
            identifiers.append(case.attrib['unique-id'])
            test_failures = case.findall('failure') + case.findall('error')
            failed_tests += bool(test_failures)
            for failure in test_failures:
                emit('test_failed', class_name=case.get('classname', owner),
                     test_name=case.get('name', case.attrib['unique-id']),
                     message=failure.get('message', ''))
                if failure.text:
                    print(failure.text.rstrip(), flush=True)
            skipped += case.find('skipped') is not None
        ET.ElementTree(suite).write(reports / report.name, encoding='utf-8', xml_declaration=True)
    return identifiers, failed_tests, skipped


def discover(configuration, directory, candidates, selected):
    def interrupted(number, frame):
        raise QueueInterrupted('JUnit discovery interrupted by signal ' + str(number))

    handlers = {number: signal.signal(number, interrupted)
                for number in (signal.SIGTERM, signal.SIGINT)}
    process = None
    try:
        with (directory / 'discovery.log').open('wb') as output:
            # Java and arguments come from trusted Gradle configuration; no shell is used.
            # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-tainted-env-args.dangerous-subprocess-use-tainted-env-args
            process = subprocess.Popen(java_command(configuration, directory,  # nosec B603
                                                  ['discover', str(candidates), str(selected)]),
                                       cwd=configuration['working_directory'],
                                       env={**os.environ, **configuration.get('environment', {})},
                                       stdout=output, stderr=subprocess.STDOUT)
            require(process.wait() == 0, 'JUnit discovery failed; see ' + str(directory / 'discovery.log'))
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=WORKER_SHUTDOWN_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for number, handler in handlers.items():
            signal.signal(number, handler)


def execute(configuration):
    reports = Path(configuration['reports_directory'])
    reports.mkdir(parents=True, exist_ok=True)
    # Only this test task's previous JUnit XML; never delete an arbitrary directory.
    for report in reports.glob('TEST-*.xml'):
        report.unlink()
    directory = Path(configuration['run_directory'])
    directory.mkdir(parents=True, exist_ok=False)
    candidates = directory / 'candidates.txt'
    candidates.write_text('\n'.join(configuration['candidates']) + '\n')
    selected = directory / 'classes.txt'
    discover(configuration, directory, candidates, selected)
    classes = selected.read_text().splitlines()
    require(len(classes) == len(set(classes)), 'Duplicate classes discovered')
    require(classes or any(configuration.get('filters', {}).values()), 'No test classes discovered')
    durations = load_durations(Path(configuration['durations_file']))
    classes = ordered_classes(classes, durations)
    (directory / 'order.json').write_text(json.dumps(classes, indent=2))
    emit('starting', task=configuration['task'], workers=configuration['workers'],
         max_heap=configuration['max_heap'], classes=len(classes), directory=str(directory))
    started = time.monotonic()
    completed, failures, events = run_workers(configuration, directory, classes)
    if configuration.get('jacoco_argument'):
        for event in events:
            if event['event'] == 'worker_started':
                recording = directory / ('worker-' + str(event['worker'])) / 'jacoco.exec'
                if not recording.is_file():
                    failures.append({'message': 'Missing JaCoCo execution data',
                                     'worker': event['worker']})
    identifiers, failed_tests, skipped = collect_reports(
        directory, reports, completed, failures, configuration.get('show_standard_streams', False))
    require(len(identifiers) == len(set(identifiers)), 'Duplicate test identities')
    missing = set((directory / 'classes.txt.static-ids').read_text().splitlines()) - set(identifiers)
    if missing or set(completed) != set(classes):
        failures.append({'message': 'Incomplete test execution', 'missing_static_test_ids': sorted(missing),
                         'incomplete_classes': sorted(set(classes) - set(completed))})
    if failures:
        write_infrastructure_failure(reports, failures)
    result = {'task': configuration['task'], 'workers': configuration['workers'],
              'max_heap': configuration['max_heap'], 'tests': len(identifiers),
              'failures': failed_tests, 'skipped': skipped, 'infrastructure_failures': failures,
              'seconds': time.monotonic() - started, 'completed_classes': len(completed),
              'selected_classes': len(classes), 'success': not failures and failed_tests == 0}
    (directory / 'result.json').write_text(json.dumps(result, indent=2))
    if result['success']:
        durations.update(completed)
        duration_file = Path(configuration['durations_file'])
        duration_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = duration_file.with_suffix('.tmp')
        temporary.write_text(json.dumps({'durations_seconds': durations}, indent=2, sort_keys=True))
        temporary.replace(duration_file)
    (directory / 'identifiers.txt').write_text('\n'.join(sorted(identifiers)) + '\n')
    with (directory / 'classes.jsonl').open('w') as output:
        for event in events:
            if event['event'] == 'done':
                output.write(json.dumps(event) + '\n')
    emit('finished', **result)
    return 0 if result['success'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('configuration', type=Path)
    arguments = parser.parse_args()
    configuration = json.loads(arguments.configuration.read_text())
    try:
        return execute(configuration)
    except Exception as error:
        emit('failed', message=str(error))
        reports = Path(configuration['reports_directory'])
        reports.mkdir(parents=True, exist_ok=True)
        write_infrastructure_failure(reports, [{'message': str(error)}])
        return 1
    finally:
        for path in Path(configuration['run_directory']).glob('worker-*/output.log'):
            with path.open('rb') as source, gzip.open(str(path) + '.gz', 'wb') as target:
                shutil.copyfileobj(source, target)
            path.unlink()


if __name__ == '__main__':
    sys.exit(main())
