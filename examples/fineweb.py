"""
This file contains the code used to process and create the
FineWeb dataset (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""

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

"""
    we first ran the following pipeline for each dump
"""
DUMP_TO_PROCESS = "CC-MAIN-2023-50"  # example

MAIN_OUTPUT_PATH = "/home/u231360/fineweb-TC"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/base_processing"
if __name__ == '__main__':
    main_processing_executor = LocalPipelineExecutor(
        pipeline=[
            WarcReader(
                f"https://data.commoncrawl.org/crawl-data/{DUMP_TO_PROCESS}/warc.paths.gz",
                glob_pattern="*/warc/*",  # we want the warc files
                default_metadata={"dump": DUMP_TO_PROCESS},
            ),
            URLFilter(),
            Trafilatura(favour_precision=True, timeout=2.),
            LanguageFilter(languages=[Languages.chinese_traditional], backend="fastlangid"),
            GopherRepetitionFilter(
                language=Languages.chinese_traditional
            ),
            GopherQualityFilter(
                language=Languages.chinese_traditional,
                min_avg_word_length=1,
                max_avg_word_length=5,
                min_stop_words=10,
                max_non_alpha_words_ratio=0.75
            ),
            C4QualityFilter(
                filter_no_terminal_punct=False,
                language=Languages.chinese_traditional
            ),
            FineWebQualityFilter(
                language=Languages.chinese_traditional,
                short_line_length=100,
                short_line_thr=0.51
            ),
            ParquetWriter(f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"),
        ],
        tasks=8000,
        logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP_TO_PROCESS}",
        randomize_start_duration=180,  # don't hit the bucket all at once with the list requests
        workers=64,
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
        f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"
    )  # this is the output from the first part

    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    stage1 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            MinhashDedupSignature(
                output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures", config=minhash_config,
                language=Languages.chinese_traditional
            ),
        ],
        tasks=TOTAL_TASKS,
        logging_dir=f"{S3_LOGS_FOLDER}/signatures",
        randomize_start_duration=180,
        depends=main_processing_executor,  # only start after the first one completes
        workers=64,
    )

    stage2 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupBuckets(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
                config=minhash_config,
            ),
        ],
        tasks=minhash_config.num_buckets * 50,  # the code supports parallelizing each bucket. here we run 50
        # workers per bucket
        randomize_start_duration=180,
        logging_dir=f"{S3_LOGS_FOLDER}/buckets",
        depends=stage1,
        workers=64,
    )


    stage3 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupCluster(
                input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
                output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids",
                config=minhash_config,
            ),
        ],
        tasks=1,  # this step runs on a single task
        logging_dir=f"{S3_LOGS_FOLDER}/clustering",
        depends=stage2,
        workers=64,
    )


    stage4 = LocalPipelineExecutor(
        pipeline=[
            INPUT_READER,
            TokensCounter(),  # you can remove this one, it's just a nice way to know how many tokens we have
            # before and after dedup
            MinhashDedupFilter(input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids"),
            # run the PII removal
            PIIFormatter(),
            ParquetWriter(f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/deduped_output"),
        ],
        tasks=TOTAL_TASKS,
        logging_dir=f"{S3_LOGS_FOLDER}/filtering",
        depends=stage3,
        workers=64,
    )

    # launch dedup pipelines
    stage4.run()
