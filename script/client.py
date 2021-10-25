import sys
import argparse
from uuid import uuid4
from time import time

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
from loguru import logger


logger.add('./logs/logger_info.log', level="INFO")
logger.add('./logs/logger_debug.log', level="DEBUG")


class CassandraBenchamarking:
    def __init__(self, cluster_ips: list, keyspace: str):
        logger.info(f"Establishing connection to {cluster_ips} - {keyspace}")

        self.cluster = Cluster(cluster_ips)
        self.session = self.cluster.connect(keyspace)
        logger.info("Session connected")
        self.session.row_factory = tuple_factory

    def execute_query(self, query: str, verbose: bool = False):
        if verbose:
            logger.debug(f"Executing query: {query}")
        result = self.session.execute(query)
        if not result:
            logger.warning("Query returned with no results")
        return result

    @staticmethod
    def get_query_from_csv(csv_filepath: str, separator: str = ';'):
        df = pd.read_csv(csv_filepath, separator)
        return df['query'].values

    def run_tests(self, query_list: list, n_iterations: int):
        unique_str = uuid4()
        logger.info(f"Running tests - {unique_str}")

        # Running each query
        for i, query in enumerate(query_list):
            logger.debug(f"Running query {i+1}/{len(query_list)}")
            logger.debug(query)

            iter_time_list = []
            # Running n_iterations for current query
            for iter in range(1, n_iterations + 1):
                logging_message = f"Running iteration {iter}/{n_iterations}"
                decimal_fraction = int(n_iterations / 10)
                if n_iterations >= 10:
                    if iter % decimal_fraction == 0:
                        logger.debug(logging_message)
                else:
                    logger.debug(logging_message)

                # Logging execution time for iteration
                start_time = time()
                self.execute_query(query)
                total_time = time() - start_time
                iter_time_list.append(total_time)

            # Creating temporary DataFrame
            df = pd.DataFrame([iter_time_list], index=[i+1])
            df['query'] = query

            # Appending query times to CSV file
            self.append_time_df_to_csv(
                df, f'./outputs/output_{unique_str}.csv')
        logger.info("Test done")

    @staticmethod
    def append_time_df_to_csv(time_df: pd.DataFrame, csv_filepath: str):
        logger.debug("Appending to output file")
        with open(csv_filepath, 'a') as f:
            time_df.to_csv(f, header=f.tell() == 0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.shutdown()
        logger.info("Connection closed")


def parse_args(args: list):
    parser = argparse.ArgumentParser(
        description='Args for Cassandra Benchmarking'
    )
    parser.add_argument(
        '-CLUSTER_IPS',
        type=list,
        nargs="+",
        help='Number of iterations for each query',
        default=['35.203.92.156']
    )
    parser.add_argument(
        '-N_ITER',
        type=int,
        help='Cluster IPs for',
        default=1000
    )
    parser.add_argument(
        '-KEYSPACE',
        type=str,
        help='Keyspace for session',
        default="microenem"
    )
    parser.add_argument(
        '-INPUT_CSV',
        type=str,
        help='CSV file for input queries',
        default='test.csv'
    )
    args = parser.parse_args(args)
    return args


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])

    CLUSTER_IPS = args.CLUSTER_IPS
    KEYSPACE = args.KEYSPACE
    N_ITER = args.N_ITER
    INPUT_CSV = args.INPUT_CSV

    with CassandraBenchamarking(CLUSTER_IPS, KEYSPACE) as db:
        queries = db.get_query_from_csv(INPUT_CSV)

        db.run_tests(queries, N_ITER)
