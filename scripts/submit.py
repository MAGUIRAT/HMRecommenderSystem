import os

from recsys.dataloaders.file import ParquetLoader, CsvLoader
from recsys.engine.submission import get_submission
from scripts.settings import (
    DATA_ROOT_DIR,
    SUBMISSION_PATH,
    RECOMMENDATION_INFERENCE_PATH,
    SUBMISSION_FILE_NAME
)

parquet_loader = ParquetLoader(DATA_ROOT_DIR)
csv_loader = CsvLoader(DATA_ROOT_DIR)


def make_submission(*directories, output_dirs, output_file_name):
    submission = parquet_loader.load_data_frame(*directories)
    submission = submission.loc[:, ["customer_id", "article_id", "score"]]
    submission = get_submission(submission)
    mask = submission.loc[:, "prediction"].isnull()
    submission.loc[mask, "prediction"] = submission.loc[~mask, "prediction"].sample(mask.sum()).values
    path = csv_loader.make_path(*output_dirs)
    path = os.path.join(path, output_file_name)
    submission.to_csv(path_or_buf=path, sep=",", header=True, index=False)


if __name__ == '__main__':
    make_submission(*RECOMMENDATION_INFERENCE_PATH,
                    output_dirs=SUBMISSION_PATH,
                    output_file_name=SUBMISSION_FILE_NAME)
