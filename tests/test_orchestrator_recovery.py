from app.etl.orchestrator import ETLOrchestrator, _close_session_best_effort


class BrokenSession:
    def close(self):
        raise RuntimeError("rollback falhou")


class RecordingLogger:
    def __init__(self):
        self.messages = []

    def warning(self, message):
        self.messages.append(message)


def test_close_session_best_effort_does_not_propagate_cleanup_error():
    logger = RecordingLogger()

    _close_session_best_effort(BrokenSession(), logger, "de carga")

    assert len(logger.messages) == 1
    assert "de carga" in logger.messages[0]
    assert "rollback falhou" in logger.messages[0]


def test_retry_resumes_raw_import_promotion():
    orchestrator = object.__new__(ETLOrchestrator)
    orchestrator._promotion_started = True
    orchestrator._resume_raw_import_promotion = lambda: "resumed"

    assert orchestrator.run() == "resumed"
