import pytest

from openeogeotrellis.util.math import logarithmic_round


class TestLogarithmicRound:
    def test_base(self):
        assert logarithmic_round(1) == 1
        assert logarithmic_round(10) == 10
        assert logarithmic_round(0.1) == 0.1

        assert logarithmic_round(2, base=2) == 2
        assert logarithmic_round(8, base=2) == 8
        assert logarithmic_round(0.25, base=2) == 0.25

    def test_zero(self):
        assert logarithmic_round(0) == 0

    @pytest.mark.parametrize(
        ["xs", "base", "delta", "expected_levels"],
        [
            (range(100, 1000), 10, 0.1, 11),
            (range(100, 1000), 10, 0.01, 101),
            (range(8, 32), 2, 0.01, 24),
        ],
    )
    def test_quantization_and_error(self, xs, base, delta, expected_levels):
        ys = [logarithmic_round(x, base=base, delta=delta) for x in xs]
        assert len(set(ys)) == expected_levels

        # Max error is proportional to delta
        max_error = max(abs(x - y) / x for (x, y) in zip(xs, ys))
        assert max_error < 1.3 * delta

    def test_sign(self):
        assert logarithmic_round(-10) == -logarithmic_round(10)
        assert logarithmic_round(-12.34) == -logarithmic_round(12.34)

    def test_general(self):
        assert 0.4 < logarithmic_round(0.5, delta=0.1) < 0.6
        assert 0.49 < logarithmic_round(0.5, delta=0.01) < 0.51

        assert logarithmic_round(0.5, delta=0.1) == logarithmic_round(0.51, delta=0.1)
        assert logarithmic_round(0.5, delta=0.01) < logarithmic_round(0.51, delta=0.01)
        assert logarithmic_round(0.5, delta=0.01) == logarithmic_round(0.501, delta=0.01)

        assert logarithmic_round(500, delta=0.1) == logarithmic_round(510, delta=0.1)
        assert logarithmic_round(500, delta=0.1) < logarithmic_round(510, delta=0.01)
        assert logarithmic_round(500, delta=0.1) == logarithmic_round(501, delta=0.01)

        assert 0.7 < logarithmic_round(0.8, delta=0.1) < 0.9
        assert 0.79 < logarithmic_round(0.8, delta=0.01) < 0.81
