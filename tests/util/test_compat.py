from openeogeotrellis.util.compat import function_supports_kwargs


def test_function_supports_kwargs():

    def fun1(x: int):
        return x

    def fun2(y: int, **kwargs):
        return y

    def fun3(y: int, **quarchz):
        return y

    assert function_supports_kwargs(fun1) is False
    assert function_supports_kwargs(fun2) is True
    assert function_supports_kwargs(fun3) is True

    assert function_supports_kwargs(lambda: 2) is False
    assert function_supports_kwargs(lambda x: x) is False
    assert function_supports_kwargs(lambda kwargs: 3) is False
    assert function_supports_kwargs(lambda **k: k) is True
