#!/usr/bin/env impala-python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This test suite validates hive serde scanner (HdfsHiveSerdeScanner) by running
# queries against text file format and
# their permutations (e.g. compression codec/compression type).
# It also tests combinations of different batch sizes and range lengths.
# TODO: add more tests

import os
from copy import deepcopy
from subprocess import call

from tests.common.impala_test_suite import *

@pytest.mark.execute_serially
class TestScanAllTypes(ImpalaTestSuite):
  TEST_TBL_NAME = 'serde_alltypes'

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    MAX_SCAN_RANGE_LENGTHS = [1, 2, 5, 16, 17, 32]
    super(TestScanAllTypes, cls).add_test_dimensions()
    # Only test text format
    cls.TestMatrix.add_dimension(
        TestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')

  def setup_method(self, method):
    test_file_path = os.path.join(os.environ['IMPALA_HOME'],
                                  'testdata/HiveSerDe/serde_alltypes.txt');
    # TODO: add test for decimal once the FE support it
    # TODO: think about how to test binary (RegexSerDe doesn't support binary)
    create_table_stmt = """
    USE functional;
    DROP TABLE IF EXISTS {0};
    CREATE TABLE {0} (
    bool_col BOOLEAN,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    string_col STRING,
    timestamp_col TIMESTAMP)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES ('input.regex' =
    '^([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)')
    STORED AS TEXTFILE;
    LOAD DATA LOCAL INPATH '{1}' OVERWRITE INTO TABLE {0};
    """.format(self.TEST_TBL_NAME, test_file_path)

    call(["hive", "-e", create_table_stmt])
    self.execute_query("invalidate metadata functional.%s" % (self.TEST_TBL_NAME))

  def teardown_method(self, method):
    call(["hive", "-e", "DROP TABLE functional.%s" % (self.TEST_TBL_NAME)])

  def test_scan(self, vector):
    vector.get_value('exec_option')['max_scan_range_length'] =\
        vector.get_value('max_scan_range_length')
    self.run_test_case('QueryTest/hive_serde', vector)
