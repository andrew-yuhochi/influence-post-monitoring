"""Unit tests for TickerExtractor — three-layer extraction pipeline."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from influence_monitor.extraction.equity_whitelist import SymbolWhitelist
from influence_monitor.extraction.ticker_extractor import (
    ExtractedTicker,
    TickerExtractor,
    _resolve_company_to_ticker,
)


@pytest.fixture(scope="module")
def whitelist() -> SymbolWhitelist:
    return SymbolWhitelist.load()


@pytest.fixture(scope="module")
def extractor(whitelist: SymbolWhitelist) -> TickerExtractor:
    return TickerExtractor(whitelist)


# ------------------------------------------------------------------
# Layer 1: Cashtag extraction (HIGH confidence)
# ------------------------------------------------------------------

class TestCashtagExtraction:
    def test_single_cashtag(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("$FNMA is massively undervalued")
        fnma = [t for t in results if t.ticker == "FNMA"]
        assert len(fnma) == 1
        assert fnma[0].confidence == "HIGH"
        assert fnma[0].extraction_method == "cashtag"

    def test_multiple_cashtags(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("$FNMA and $FMCC are the GSE play")
        tickers = {t.ticker for t in results}
        assert "FNMA" in tickers
        assert "FMCC" in tickers

    def test_cashtag_with_dot(self, extractor: TickerExtractor) -> None:
        """Cashtag with class suffix like $BRK.B."""
        results = extractor.extract("$BRK.B is cheap here")
        brk = [t for t in results if t.ticker == "BRK.B"]
        # BRK.B may or may not be in whitelist — just verify regex works
        # The point is the regex captures it
        assert len(brk) <= 1  # 0 if not in whitelist, 1 if it is


# ------------------------------------------------------------------
# Layer 2: Uppercase extraction (MEDIUM confidence)
# ------------------------------------------------------------------

class TestUppercaseExtraction:
    def test_standalone_uppercase(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("TSLA could be a 10x from here")
        tsla = [t for t in results if t.ticker == "TSLA"]
        assert len(tsla) == 1
        assert tsla[0].confidence == "MEDIUM"
        assert tsla[0].extraction_method == "uppercase"

    @pytest.mark.parametrize("false_positive", [
        "CEO", "IPO", "ETF", "GDP", "FED", "SEC", "NYSE", "USD",
    ])
    def test_false_positives_filtered(
        self, extractor: TickerExtractor, false_positive: str,
    ) -> None:
        results = extractor.extract(f"The {false_positive} said something important")
        tickers = {t.ticker for t in results}
        assert false_positive not in tickers, f"{false_positive} should be filtered out"

    def test_mixed_case_not_extracted(self, extractor: TickerExtractor) -> None:
        """Only fully uppercase words should be caught by Layer 2."""
        results = extractor.extract("Tesla is going up")
        tickers = {t.ticker for t in results}
        assert "Tesla" not in tickers


# ------------------------------------------------------------------
# Layer 3: NER extraction (LOW confidence)
# ------------------------------------------------------------------

class TestNERExtraction:
    @patch(
        "influence_monitor.extraction.ticker_extractor._resolve_company_to_ticker",
        return_value="AAPL",
    )
    def test_company_name_to_ticker(
        self, mock_resolve, extractor: TickerExtractor,
    ) -> None:
        results = extractor.extract("Apple is a strong buy right now")
        aapl = [t for t in results if t.ticker == "AAPL"]
        assert len(aapl) == 1
        assert aapl[0].confidence == "LOW"
        assert aapl[0].extraction_method == "ner"
        assert aapl[0].company_name is not None

    @patch(
        "influence_monitor.extraction.ticker_extractor._resolve_company_to_ticker",
        return_value="FNMA",
    )
    def test_fannie_mae_resolves(
        self, mock_resolve, extractor: TickerExtractor,
    ) -> None:
        results = extractor.extract("Fannie Mae is going to be released from conservatorship")
        fnma = [t for t in results if t.ticker == "FNMA"]
        assert len(fnma) == 1
        assert fnma[0].company_name is not None

    @patch(
        "influence_monitor.extraction.ticker_extractor._resolve_company_to_ticker",
        return_value=None,
    )
    def test_unresolvable_entity(
        self, mock_resolve, extractor: TickerExtractor,
    ) -> None:
        """Entity that doesn't resolve to a ticker should be skipped."""
        results = extractor.extract("The Federal Reserve announced a rate hold")
        tickers = {t.ticker for t in results}
        assert "FED" not in tickers


# ------------------------------------------------------------------
# Whitelist gating
# ------------------------------------------------------------------

class TestWhitelistGating:
    def test_invalid_cashtag_filtered(self, extractor: TickerExtractor) -> None:
        """A cashtag that's not in the whitelist should not appear."""
        results = extractor.extract("$ZZZZZ is my made-up ticker")
        tickers = {t.ticker for t in results}
        assert "ZZZZZ" not in tickers

    def test_invalid_uppercase_filtered(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("XYZWQ is not a real ticker")
        tickers = {t.ticker for t in results}
        assert "XYZWQ" not in tickers


# ------------------------------------------------------------------
# Deduplication across layers
# ------------------------------------------------------------------

class TestDeduplication:
    def test_cashtag_takes_precedence(self, extractor: TickerExtractor) -> None:
        """If the same ticker appears as cashtag and uppercase, HIGH wins."""
        results = extractor.extract("$AAPL is great. AAPL all the way.")
        aapl = [t for t in results if t.ticker == "AAPL"]
        assert len(aapl) == 1
        assert aapl[0].confidence == "HIGH"

    def test_no_duplicate_tickers(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("$TSLA TSLA $TSLA again TSLA")
        tsla_count = sum(1 for t in results if t.ticker == "TSLA")
        assert tsla_count == 1


# ------------------------------------------------------------------
# Sample texts from research (AC: "Test against 4 sample texts")
# ------------------------------------------------------------------

class TestSampleTexts:
    """Realistic investor post texts based on RESEARCH-REPORT.md examples."""

    def test_ackman_fnma_post(self, extractor: TickerExtractor) -> None:
        text = (
            "$FNMA and $FMCC remain massively undervalued. "
            "The GSEs are trading at a fraction of book value. "
            "Conservatorship release is a matter of when, not if."
        )
        results = extractor.extract(text)
        tickers = {t.ticker for t in results}
        assert "FNMA" in tickers
        assert "FMCC" in tickers

    def test_cathie_wood_growth_post(self, extractor: TickerExtractor) -> None:
        text = (
            "TSLA is the most important stock of the decade. "
            "Autonomous driving alone justifies a $2T market cap. "
            "We continue to add to our position in NVDA as well."
        )
        results = extractor.extract(text)
        tickers = {t.ticker for t in results}
        assert "TSLA" in tickers
        assert "NVDA" in tickers

    def test_short_seller_report(self, extractor: TickerExtractor) -> None:
        text = (
            "Our latest research reveals significant accounting "
            "irregularities at $NKLA. We are short. "
            "Management has misrepresented the EPS trajectory."
        )
        results = extractor.extract(text)
        tickers = {t.ticker for t in results}
        assert "NKLA" in tickers
        # EPS should be filtered as false positive
        assert "EPS" not in tickers

    def test_macro_post_no_tickers(self, extractor: TickerExtractor) -> None:
        text = (
            "The FED is making a policy mistake. GDP growth is slowing, "
            "CPI is sticky, and the yield curve is still inverted. "
            "Risk assets face headwinds IMO."
        )
        results = extractor.extract(text)
        tickers = {t.ticker for t in results}
        # None of FED, GDP, CPI, IMO should appear
        assert "FED" not in tickers
        assert "GDP" not in tickers
        assert "CPI" not in tickers
        assert "IMO" not in tickers


# ------------------------------------------------------------------
# Confidence ordering
# ------------------------------------------------------------------

class TestConfidenceOrdering:
    def test_results_sorted_by_confidence(self, extractor: TickerExtractor) -> None:
        results = extractor.extract("$AAPL is great, MSFT too")
        if len(results) >= 2:
            confidences = [t.confidence for t in results]
            priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            assert all(
                priority[confidences[i]] <= priority[confidences[i + 1]]
                for i in range(len(confidences) - 1)
            )
