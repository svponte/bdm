import argparse
import os
import sys
from time import time
from uuid import uuid4

import cassandra
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import tuple_factory
from loguru import logger

logger.add('./logs/logger_info.log', level="INFO")
logger.add('./logs/logger_debug.log', level="DEBUG")


class CassandraBenchamarking:
    def __init__(self, cluster_ips: list, keyspace: str):
        """Establishes connection with the Cluster and keyspace

        :param cluster_ips: Cluster's IP list
        :type cluster_ips: list
        :param keyspace: Keyspace to connect to
        :type keyspace: str
        :raises ConnectionError: If connection was not succesful
        """
        logger.info(f"Establishing connection to {cluster_ips} - {keyspace}")

        self._session = None
        self._cluster = Cluster(cluster_ips)

        try:
            self._session = self._cluster.connect(keyspace)
        except NoHostAvailable:
            message = f"Host {cluster_ips} not available"
            logger.error(message)
            raise ConnectionError(message)
        logger.info("Session connected")

        if self._session:
            self._session.row_factory = tuple_factory

    def execute_query(self, query: str, timeout: int = 20, verbose: bool = False):
        """Executes a single query

        :param query: Query to execute
        :type query: str
        :param timeout: Query timeout, defaults to 20
        :type timeout: int, optional
        :param verbose: If verbose messages should be shown, defaults to False
        :type verbose: bool, optional
        :raises ValueError: If there was a problem to execute the query
        :return: Result object
        """
        if verbose:
            logger.debug(f"Executing query: {query}")
        try:
            result = self._session.execute(query, timeout=timeout)
        except cassandra.InvalidRequest:
            message = "Invalid query"
            logger.error(message)
            raise ValueError(message)
        except cassandra.OperationTimedOut:
            message = "OperationTimedOut while executing query."
            logger.error(message)
            raise ValueError(message)
        except cassandra.ReadFailure:
            message = "ReadFailure while executing query."
            logger.error(message)
            raise ValueError(message)

        if not result:
            logger.warning("Query returned with no results")
        return result

    @staticmethod
    def get_query_from_csv(csv_filepath: str, separator: str = ';') -> np.array:
        """Get queries list from a CSV file. They are executed on both tests

        :param csv_filepath: Filepath to CSV file
        :type csv_filepath: str
        :param separator: Default CSV separator, defaults to ';'
        :type separator: str, optional
        :return: List of queries
        :rtype: np.array
        """
        df = pd.read_csv(csv_filepath, separator)
        return df['query'].values

    def run_tests_iterations(self, query_list: list, n_iterations: int):
        """Test based on repeated iterations. A list of queries from CSV file \
            are executed n_iterations times and each execution time is logged and saved in \
            output CSV file

        :param query_list: Input list of queries to be executed
        :type query_list: list
        :param n_iterations: Number of times the query list is executed
        :type n_iterations: int
        """
        test_starting_time = time()
        unique_str = uuid4()
        logger.info(f"Running tests - {unique_str}")

        # Running n_iterations for current query
        for iter in range(1, n_iterations + 1):
            logging_message = f"Running iteration {iter}/{n_iterations}"

            # Control log every 10th part of the iterations
            decimal_fraction = int(n_iterations / 10)
            if n_iterations >= 10:
                if iter % decimal_fraction == 0:
                    logger.debug(logging_message)
            else:
                logger.debug(logging_message)

            # Running each query
            null_counter = 0
            iter_time_list = []
            for i, query in enumerate(query_list):
                logger.debug(f"Running query {i+1}/{len(query_list)}")
                logger.debug(query)

                # Logging execution time for iteration
                start_time = time()
                result = self.execute_query(query)
                if not result:
                    null_counter += 1
                total_time = time() - start_time
                iter_time_list.append(total_time)

            # Creating temporary DataFrame
            df = pd.DataFrame([iter_time_list],
                              columns=query_list, index=[iter])

            # Appending query times to CSV file
            self.append_time_df_to_csv(
                df, f'./outputs/output_{unique_str}.csv')

        # Execution summary
        test_ending_time = time() - test_starting_time
        logger.info(f"Test {unique_str} done in {test_ending_time} seconds")
        logger.info(
            f"Total of {n_iterations} iterations with {len(query_list)} queries ({null_counter} null) each")

    def run_tests_queries(self, query_list: list):
        """Test based on the single execution of a query list. Each query on the list \
            is executed once and the execution time is saved on a output CSV file.

        :param query_list: Input query list to be executed once each
        :type query_list: list
        """
        n_queries = len(query_list)

        test_starting_time = time()
        unique_str = uuid4()
        logger.info(f"Running tests - {unique_str}")

        # Running each query
        null_counter = 0
        iter_time_list = []
        for i, query in enumerate(query_list):
            logging_msg = f"Running query {i+1}/{n_queries}"

            # Control log every 10th part of the queries
            decimal_fraction = int(n_queries / 10)
            if n_queries >= 10:
                if i % decimal_fraction == 0:
                    logger.info(logging_msg)
            else:
                logger.info(logging_msg)

            # Logging execution time for query
            start_time = time()
            result = self.execute_query(query, verbose=True)
            if not result:
                null_counter += 1
            total_time = time() - start_time
            iter_time_list.append(total_time)

        # Creating output DataFrame
        df = pd.DataFrame(iter_time_list, 
                          index=query_list, 
                          columns=['execution_time'])

        # Appending query times to CSV file
        df.to_csv(f'./outputs/output_{unique_str}.csv')

        # Execution summary
        test_ending_time = time() - test_starting_time
        logger.info(f"Test {unique_str} done in {test_ending_time} seconds")
        logger.info(f"Total of {n_queries} queries ({null_counter} null) each")

    @staticmethod
    def append_time_df_to_csv(time_df: pd.DataFrame, csv_filepath: str):
        """Appends a DataFrame to a CSV file. This approach is useful in big tests \
            since the results are saved at the end of each query list instead of at the end

        :param time_df: Input Dataframe
        :type time_df: pd.DataFrame
        :param csv_filepath: CSV file to append the input Dataframe
        :type csv_filepath: str
        """
        logger.debug("Appending to output file")
        if 'outputs' not in os.listdir():
            os.mkdir('./outputs')
        with open(csv_filepath, 'a') as f:
            time_df.to_csv(f, header=f.tell() == 0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            self._session.shutdown()
            self._session = None
        logger.info("Connection closed")


def parse_args(args: list):
    parser = argparse.ArgumentParser(
        description='Args for Cassandra Benchmarking'
    )
    parser.add_argument(
        '-CLUSTER_IPS',
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
    parser.add_argument(
        '-TEST_TYPE',
        type=str,
        help="Type of test to used: iteration ('iter') or query based ('query')",
        default='query'
    )
    args = parser.parse_args(args)
    return args


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])

    CLUSTER_IPS = args.CLUSTER_IPS
    KEYSPACE = args.KEYSPACE
    N_ITER = args.N_ITER
    INPUT_CSV = args.INPUT_CSV
    TEST_TYPE = args.TEST_TYPE

    with CassandraBenchamarking(CLUSTER_IPS, KEYSPACE) as db:
        queries = db.get_query_from_csv(INPUT_CSV)
        if TEST_TYPE == 'iter':
            db.run_tests_iterations(queries, N_ITER)
        elif TEST_TYPE == 'query':
            db.run_tests_queries(queries)
        else:
            msg = "Invalid test type. Choose either 'query' or 'iter'"
            raise ValueError(msg)
