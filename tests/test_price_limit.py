"""Price-limit band and one-line-board (一字板) detection tests."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pulsar_contracts import Bar, Board, Freq

from pulsar_exec import MatchingRules, is_one_line_board, limit_prices

DAY = date(2026, 10, 5)


def bar(open_: float, high: float, low: float, close: float, volume: float = 10_000.0):
    return Bar(
        symbol="600519",
        ts=datetime(2026, 10, 5),
        freq=Freq.DAILY,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=close * volume,
    )


class TestLimitBands:
    def test_main_board_ten_percent(self):
        # 10.00 * 1.10 = 11.00 ; 10.00 * 0.90 = 9.00
        up, down = limit_prices(10.00, board=Board.MAIN, is_st=False, rules=MatchingRules())
        assert up == pytest.approx(11.00)
        assert down == pytest.approx(9.00)

    def test_rounding_is_half_up_to_the_cent(self):
        # 9.87 * 1.10 = 10.857 -> 10.86 ; 9.87 * 0.90 = 8.883 -> 8.88
        up, down = limit_prices(9.87, board=Board.MAIN, is_st=False, rules=MatchingRules())
        assert up == pytest.approx(10.86)
        assert down == pytest.approx(8.88)

    def test_gem_and_star_twenty_percent(self):
        # 5.00 * 1.20 = 6.00 ; 5.00 * 0.80 = 4.00
        for board in (Board.GEM, Board.STAR):
            up, down = limit_prices(5.00, board=board, is_st=False, rules=MatchingRules())
            assert up == pytest.approx(6.00)
            assert down == pytest.approx(4.00)

    def test_st_narrows_main_board_to_five_percent(self):
        # 20.00 * 1.05 = 21.00 ; 20.00 * 0.95 = 19.00
        up, down = limit_prices(20.00, board=Board.MAIN, is_st=True, rules=MatchingRules())
        assert up == pytest.approx(21.00)
        assert down == pytest.approx(19.00)

    def test_st_does_not_narrow_gem(self):
        # Post-registration-reform GEM keeps ±20% even for ST labels.
        up, down = limit_prices(5.00, board=Board.GEM, is_st=True, rules=MatchingRules())
        assert up == pytest.approx(6.00)
        assert down == pytest.approx(4.00)

    def test_rates_are_configurable(self):
        rules = MatchingRules(
            price_limit_rates={Board.MAIN: 0.30, Board.GEM: 0.20, Board.STAR: 0.20, Board.BSE: 0.30}
        )
        up, down = limit_prices(10.00, board=Board.MAIN, is_st=False, rules=rules)
        assert up == pytest.approx(13.00)
        assert down == pytest.approx(7.00)

    def test_invalid_prev_close_rejected(self):
        with pytest.raises(ValueError):
            limit_prices(0.0, board=Board.MAIN, is_st=False, rules=MatchingRules())


class TestOneLineBoard:
    def test_sealed_limit_up_is_one_line(self):
        # prev close 10.00 -> limit up 11.00; bar locked at 11.00 all day.
        assert is_one_line_board(
            bar(11.00, 11.00, 11.00, 11.00),
            10.00,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )

    def test_sealed_limit_down_is_one_line(self):
        assert is_one_line_board(
            bar(9.00, 9.00, 9.00, 9.00),
            10.00,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )

    def test_locked_bar_off_the_band_is_not_one_line(self):
        # Locked at 10.50 — an illiquid day, but not the limit band.
        assert not is_one_line_board(
            bar(10.50, 10.50, 10.50, 10.50),
            10.00,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )

    def test_bar_with_range_at_limit_close_is_not_one_line(self):
        # Touched the limit during the day but traded a range: not sealed.
        assert not is_one_line_board(
            bar(10.20, 11.00, 10.10, 11.00),
            10.00,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )

    def test_unknown_prev_close_locked_bar_is_conservatively_sealed(self):
        assert is_one_line_board(
            bar(10.50, 10.50, 10.50, 10.50),
            None,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )

    def test_st_band_uses_five_percent(self):
        # ST main board: 10.00 -> band [9.50, 10.50]; locked at 10.50 seals.
        assert is_one_line_board(
            bar(10.50, 10.50, 10.50, 10.50),
            10.00,
            board=Board.MAIN,
            is_st=True,
            rules=MatchingRules(),
        )
        # non-ST would cap at 11.00, so 10.50 is not a sealed board there
        assert not is_one_line_board(
            bar(10.50, 10.50, 10.50, 10.50),
            10.00,
            board=Board.MAIN,
            is_st=False,
            rules=MatchingRules(),
        )
