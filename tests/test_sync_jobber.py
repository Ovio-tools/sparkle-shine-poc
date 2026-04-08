from intelligence.syncers.sync_jobber import _quote_is_recurring


def test_quote_is_recurring_when_any_linked_job_is_recurring():
    quote = {
        "jobs": {
            "nodes": [
                {"jobType": "ONE_OFF"},
                {"jobType": "RECURRING"},
            ]
        }
    }

    assert _quote_is_recurring(quote) is True


def test_quote_is_not_recurring_without_recurring_linked_jobs():
    quote = {"jobs": {"nodes": [{"jobType": "ONE_OFF"}]}}

    assert _quote_is_recurring(quote) is False
