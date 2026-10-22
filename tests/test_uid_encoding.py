"""Unit tests for the iCalendar UID -> PidLidGlobalObjectId encoding.

Loads ``event_utils`` straight from its file (no Home Assistant needed).
The equivalence test against exchangelib's own ``UID`` helper runs only when
exchangelib is importable and is skipped otherwise.
"""
import importlib.util
import struct
from pathlib import Path

import pytest

_MOD_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "exchange_calendar"
    / "event_utils.py"
)
_spec = importlib.util.spec_from_file_location("event_utils", _MOD_PATH)
eu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eu)

OUTLOOK_UID = (
    "040000008200E00074C5B7101A82E00800000000F04B39141EFCDC01"
    "000000000000000010000000FE6755D9EA45EF46BE243D2CD880EC1C"
)
VCAL_UID = "261cbc18-1f65-5a0a-bd11-23b1e224cc2f"


def test_outlook_style_uid_is_decoded_as_is():
    goid = eu.uid_to_global_object_id(OUTLOOK_UID)
    assert goid == bytes.fromhex(OUTLOOK_UID)
    assert goid.startswith(eu.GLOBAL_OBJECT_ID_HEADER)


def test_outlook_prefix_is_case_insensitive():
    assert eu.uid_to_global_object_id(OUTLOOK_UID.lower()) == bytes.fromhex(OUTLOOK_UID)


def test_vcal_uid_is_wrapped_in_envelope():
    goid = eu.uid_to_global_object_id(VCAL_UID)
    header_len = 16 + 4 + 8 + 8
    assert goid[:16] == eu.GLOBAL_OBJECT_ID_HEADER
    assert goid[16:header_len] == bytes(20)
    payload = b"vCal-Uid\x01\x00\x00\x00" + VCAL_UID.encode("ascii") + b"\x00"
    (length,) = struct.unpack("<I", goid[header_len : header_len + 4])
    assert length == len(payload)
    assert goid[header_len + 4 :] == payload


def test_odd_length_hex_prefix_falls_back_to_envelope():
    # Looks like an Outlook UID but is not valid hex -> treated as a plain UID.
    uid = eu.OUTLOOK_UID_PREFIX + "ABC"
    goid = eu.uid_to_global_object_id(uid)
    assert goid is not None and b"vCal-Uid" in goid


def test_empty_or_non_ascii_uid_returns_none():
    assert eu.uid_to_global_object_id("") is None
    assert eu.uid_to_global_object_id(None) is None
    assert eu.uid_to_global_object_id("ünïcödé-uid") is None


def test_matches_exchangelib_uid_helper():
    exchangelib = pytest.importorskip("exchangelib")
    from exchangelib.properties import UID  # noqa: F401 - import checked above

    assert bytes(UID(VCAL_UID)) == eu.uid_to_global_object_id(VCAL_UID)
    assert UID.to_global_object_id(OUTLOOK_UID) == eu.uid_to_global_object_id(OUTLOOK_UID)
    assert exchangelib.__version__  # keep the fixture referenced
