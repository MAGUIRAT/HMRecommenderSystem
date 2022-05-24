import os
from subprocess import run

SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))


def get_python_command():
    if os.name == "nt":
        return "python "
    return "python3 "


def get_absolute_path(script):
    return os.path.join(SCRIPTS_DIR, script)


def get_kwargs_string(**kwargs):
    return " ".join([' --' + k + ' ' + str(v)
                     for k, v in kwargs.items()])


def get_command(script, **kwargs):
    return get_python_command() + \
           get_absolute_path(script) + \
           get_kwargs_string(**kwargs)


if __name__ == "__main__":
    run(get_command("preprocess.py"))
    run(get_command("repartition.py"))
    run(get_command("compute_articles_features.py"))
    run(get_command("compute_customers_features.py"))
    run(get_command("compute_train_candidates.py"))
    run(get_command("compute_inference_candidates.py"))
    run(get_command("fit.py"))
    run(get_command("predict.py"))
    run(get_command("submit.py"))
