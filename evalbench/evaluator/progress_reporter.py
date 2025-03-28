import logging
import sys
import threading
from io import StringIO
import threading
import multiprocessing

_ORIGINAL_STDOUT = sys.stdout
_ORIGINAL_STDERR = sys.stderr
_ORIGINAL_HANDLERS = logging.getLogger().handlers[:]
_NUM_LINES_FOR_PROGRESS = 5


def setup_progress_reporting(total_dataset_len: int, total_dbs: int):
    manager = multiprocessing.Manager()
    sys.stdout = tmp_buffer = StringIO()
    sys.stderr = tmp_buffer
    logger = logging.getLogger()
    logger.handlers = []
    buffer_handler = logging.StreamHandler(tmp_buffer)
    logger.addHandler(buffer_handler)
    _ORIGINAL_STDOUT.write(("-" * 80 + "\n") * _NUM_LINES_FOR_PROGRESS)
    progress_reporting = {
        "lock": manager.Lock(),
        "setup_i": manager.Value("i", 0),
        "prompt_i": manager.Value("i", 0),
        "gen_i": manager.Value("i", 0),
        "exec_i": manager.Value("i", 0),
        "score_i": manager.Value("i", 0),
        "total": total_dataset_len,
        "total_dbs": total_dbs,
    }
    progress_reporting_finished = threading.Event()
    progress_reporting_thread = threading.Thread(
        target=_report,
        args=(progress_reporting, progress_reporting_finished, tmp_buffer),
        daemon=True,
    )
    progress_reporting_thread.start()
    return (
        progress_reporting_thread,
        progress_reporting,
        progress_reporting_finished,
        tmp_buffer,
    )


def _report(progress_reporting, progress_reporting_finished, tmp_buffer):
    while not progress_reporting_finished.is_set():

        setup_i = progress_reporting["setup_i"].value
        prompt_i = progress_reporting["prompt_i"].value
        gen_i = progress_reporting["gen_i"].value
        exec_i = progress_reporting["exec_i"].value
        score_i = progress_reporting["score_i"].value
        dataset_len = progress_reporting["total"]
        databases = progress_reporting["total_dbs"]

        buffer_content = tmp_buffer.getvalue()
        tmp_buffer.seek(0)
        tmp_buffer.truncate(0)
        if buffer_content != "":
            _ORIGINAL_STDOUT.write("\n")
            _ORIGINAL_STDOUT.write(buffer_content)
            _ORIGINAL_STDOUT.write("\n" * (_NUM_LINES_FOR_PROGRESS + 1))

        _ORIGINAL_STDOUT.write("\033[F\033[K" * _NUM_LINES_FOR_PROGRESS)

        report_progress(
            setup_i, databases, prefix="DBs Setup:", suffix="Complete", length=50
        )
        report_progress(
            prompt_i, dataset_len, prefix="Prompts:  ", suffix="Complete", length=50
        )
        report_progress(
            gen_i, dataset_len, prefix="SQLGen:   ", suffix="Complete", length=50
        )
        report_progress(
            exec_i, dataset_len, prefix="SQLExec:  ", suffix="Complete", length=50
        )
        report_progress(
            score_i, dataset_len, prefix="Scoring:  ", suffix="Complete", length=50
        )
        _ORIGINAL_STDOUT.flush()

        if progress_reporting_finished.wait(timeout=3):
            break


def skip_database(sub_datasets, progress_reporter):
    evals_in_db = sum(
        len(sub_datasets.get(query_type, [])) for query_type in ["dql", "dml", "ddl"]
    )
    with progress_reporter["lock"]:
        progress_reporter["total_dbs"] -= 1
        progress_reporter["total"] -= evals_in_db


def skip_dialect(sub_datasets, progress_reporter):
    for database in sub_datasets:
        skip_database(sub_datasets[database], progress_reporter)


def cleanup_progress_reporting(tmp_buffer):
    sys.stdout = _ORIGINAL_STDOUT
    sys.stderr = _ORIGINAL_STDERR
    logger = logging.getLogger()
    logger.handlers = _ORIGINAL_HANDLERS
    sys.stdout.write(tmp_buffer.getvalue())
    tmp_buffer.close()


# Print iterations progress bar for parallel calls
def report_progress(
    iteration,
    total,
    prefix="",
    suffix="",
    decimals=1,
    length=100,
    fill="█",
    printEnd="\n",
):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)

    # Take the first 4 lines of the stdout for progress reporting
    _ORIGINAL_STDOUT.write(f"{prefix} |{bar}| {percent}% {suffix}{printEnd}")
