from utils.result import Result
import pytest


class TestInit:
    def test_init_with_valid_values(self):
        result = Result.Ok(value="ok", check_success=True)
        result2 = Result.Ok(value="ok", check_success=False)
        result3 = Result.Fail(Exception())

    def test_init_with_invalid_values(self):
        with pytest.raises(Exception):
            result = Result(value="ok", check_success=True, success=False)
        with pytest.raises(Exception):
            result = Result(
                value="ok", check_success=True, success=True, error="something"
            )


class TestCombine:
    def test_combine_without_error(self):
        result_list = [
            Result.Ok("a", True),
            Result.Ok("b", True),
            Result.Ok("c", False),
        ]
        result = Result.combine(result_list)
        assert result.success

    def test_combine_with_error(self):
        result_list = [
            Result.Ok("a", True),
            Result.Ok("b", True),
            Result.Fail("c"),
        ]
        result = Result.combine(result_list)
        assert not result.success

    def test_combine_without_error_list_result(self):
        result_list = [
            Result.Ok("a", True),
            Result.Ok("b", True),
            Result.Ok(["c", "d"], False),
        ]
        result = Result.combine(result_list)
        assert result.success
        assert result.value == ["a", "b", "c", "d"]
