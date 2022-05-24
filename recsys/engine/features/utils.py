from numpy import percentile as np_percentile


def percentile(name, n=25):
    def percentile_(x):
        return np_percentile(x, n)

    percentile_.__name__ = f'{name}'
    return percentile_
