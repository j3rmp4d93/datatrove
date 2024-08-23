"""
This file contains the code used to process and create the
FineWeb dataset (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""
import os
os.environ['OPENBLAS_NUM_THREADS'] = '2'
from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers import ParquetWriter
from datatrove.pipeline.readers import ParquetReader
from datatrove.utils.typeshelper import Languages
import argparse
parser = argparse.ArgumentParser(description='fineweb Traditional Chinese')
parser.add_argument('-dump', type=str, help='which dump of cc to process, eg. CC-MAIN-2023-50', default=None, required=True)
parser.add_argument('-num_worker', type=int, help='number of workers', default=1)
args = parser.parse_args()
"""
    we first ran the following pipeline for each dump
"""

MAIN_OUTPUT_PATH = "/home/u231360/fineweb-TC"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/base_processing"
if __name__ == '__main__':
    from fsspec.implementations.http import HTTPFileSystem
    file = HTTPFileSystem(f"https://data.commoncrawl.org/crawl-data/{args.dump}/warc.paths.gz")
    total_warcs = len(file.read_text(f"https://data.commoncrawl.org/crawl-data/{args.dump}/warc.paths.gz", compression='infer').strip().split('\n'))
    main_processing_executor = LocalPipelineExecutor(
        pipeline=[
            WarcReader(
                f"https://data.commoncrawl.org/crawl-data/{args.dump}/warc.paths.gz",
                glob_pattern="*/warc/*",  # we want the warc files
                default_metadata={"dump": args.dump},
            ),
            URLFilter(),
            Trafilatura(favour_precision=True, timeout=5.),
            LanguageFilter(languages=[Languages.chinese_traditional], backend="fastlangid"),
            GopherRepetitionFilter(
                language=Languages.chinese_traditional
            ),
            GopherQualityFilter(
                language=Languages.chinese_traditional,
                min_avg_word_length=1,
                max_avg_word_length=3,
                min_stop_words=10,
                max_non_alpha_words_ratio=0.8
            ),
            C4QualityFilter(
                filter_no_terminal_punct=False,
                language=Languages.chinese_traditional
            ),
            FineWebQualityFilter(
                language=Languages.chinese_traditional,
                short_line_length=30,
                short_line_thr=0.67
            ),
            ParquetWriter(f"{FILTERING_OUTPUT_PATH}/output/{args.dump}"),
        ],
        tasks=total_warcs,
        logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{args.dump}",
        randomize_start_duration=180,  # don't hit the bucket all at once with the list requests
        workers=args.num_worker,
    )
    main_processing_executor.run()

    """
        we then applied minhash deduplication to each individual dump,
    """

    # you can also change ngrams or the number of buckets and their size here
    minhash_config = MinhashConfig(
        num_buckets=14,
        hashes_per_bucket=8,
        n_grams=5,
    )

    S3_MINHASH_BASE_PATH = f"{MAIN_OUTPUT_PATH}/minhash"

    S3_LOGS_FOLDER = f"{MAIN_OUTPUT_PATH}/logs/minhash"
    LOCAL_LOGS_FOLDER = "logs/minhash"

    TOTAL_TASKS = 1000

    # this is the original data that we want to deduplicate
    INPUT_READER = ParquetReader(
        f"{FILTERING_OUTPUT_PATH}/output/{args.dump}"
    )  # this is the output from the first part

    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    stage1 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            MinhashDedupSignature(
                output_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/signatures", config=minhash_config,
                language=Languages.chinese_traditional
            ),
        ],
        tasks=TOTAL_TASKS,
        logging_dir=f"{S3_LOGS_FOLDER}/signatures",
        randomize_start_duration=180,
        depends=main_processing_executor,  # only start after the first one completes
        workers=args.num_worker,
    )

    stage2 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupBuckets(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/signatures",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/buckets",
                config=minhash_config,
            ),
        ],
        tasks=minhash_config.num_buckets * 50,  # the code supports parallelizing each bucket. here we run 50
        # workers per bucket
        randomize_start_duration=180,
        logging_dir=f"{S3_LOGS_FOLDER}/buckets",
        depends=stage1,
        workers=args.num_worker,
    )


    stage3 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupCluster(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/buckets",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/remove_ids",
                config=minhash_config,
            ),
        ],
        tasks=1,  # this step runs on a single task
        logging_dir=f"{S3_LOGS_FOLDER}/clustering",
        depends=stage2,
        workers=args.num_worker,
    )


    stage4 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            TokensCounter(),  # you can remove this one, it's just a nice way to know how many tokens we have
            # before and after dedup
            MinhashDedupFilter(input_folder=f"{S3_MINHASH_BASE_PATH}/{args.dump}/remove_ids"),
            # run the PII removal
            PIIFormatter(),
            ParquetWriter(f"{S3_MINHASH_BASE_PATH}/{args.dump}/deduped_output"),
        ],
        tasks=TOTAL_TASKS,
        logging_dir=f"{S3_LOGS_FOLDER}/filtering",
        depends=stage3,
        workers=args.num_worker,
    )

    # launch dedup pipelines
    stage4.run()
