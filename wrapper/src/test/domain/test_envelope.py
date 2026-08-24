"""app/domain/envelope.py 미러 — 봉투 규약."""
from app.domain.envelope import fail, ok


def test_ok_wraps_data_with_success_flag():
    assert ok({"keywords": ["k"]}) == {"success": True, "data": {"keywords": ["k"]}}


def test_fail_includes_only_present_fields():
    assert fail("busy", retry_after=2) == {
        "success": False, "error": {"code": "busy", "retry_after": 2}}
    assert fail("upstream", detail="boom") == {
        "success": False, "error": {"code": "upstream", "detail": "boom"}}
