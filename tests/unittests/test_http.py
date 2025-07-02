import pytest
from unittest.mock import MagicMock, patch
from zeep.exceptions import XMLSyntaxError, Fault, TransportError
from tap_listrak.http import request

MAX_RETRIES = 5


@pytest.fixture
def mock_http_timer():
    """Patch the http_request_timer context manager."""
    with patch("tap_listrak.http.metrics.http_request_timer") as mock_timer:
        mock_context = MagicMock()
        mock_timer.return_value.__enter__.return_value = mock_context
        yield mock_timer


def test_successful_request(mock_http_timer):
    def service_fn(**kwargs):
        return "Success"

    result = request("test_stream", service_fn)
    assert result == "Success"
    assert mock_http_timer.call_count == 1


def test_xml_syntax_error_retry(mock_http_timer):
    failing_mock = MagicMock(side_effect=XMLSyntaxError("Simulated XML error"))

    with pytest.raises(XMLSyntaxError):
        request("test_stream", failing_mock)

    assert failing_mock.call_count == MAX_RETRIES
    assert mock_http_timer.call_count == MAX_RETRIES


def test_fault_error_retry(mock_http_timer):
    failing_mock = MagicMock(side_effect=Fault("Simulated Fault"))

    with pytest.raises(Fault):
        request("test_stream", failing_mock)

    assert failing_mock.call_count == MAX_RETRIES
    assert mock_http_timer.call_count == MAX_RETRIES


def test_transport_error_retry(mock_http_timer):
    failing_mock = MagicMock(side_effect=TransportError(502, "Bad Gateway"))

    with pytest.raises(TransportError):
        request("test_stream", failing_mock)

    assert failing_mock.call_count == MAX_RETRIES
    assert mock_http_timer.call_count == MAX_RETRIES


def test_retry_recovers_before_max_attempts(mock_http_timer):
    service_mock = MagicMock()
    service_mock.side_effect = [XMLSyntaxError("fail"), "Recovered"]

    result = request("test_stream", service_mock)

    assert result == "Recovered"
    assert service_mock.call_count == 2
    assert mock_http_timer.call_count == 2


def test_unhandled_exception_not_retried(mock_http_timer):
    service_mock = MagicMock(side_effect=ValueError("unexpected"))

    with pytest.raises(ValueError):
        request("test_stream", service_mock)

    assert service_mock.call_count == 1
    assert mock_http_timer.call_count == 1


def test_none_as_service_fn(mock_http_timer):
    with pytest.raises(TypeError):
        request("test_stream", None)


def test_service_fn_returns_none(mock_http_timer):
    def service_fn(**kwargs):
        return None

    result = request("test_stream", service_fn)
    assert result is None
    assert mock_http_timer.call_count == 1
