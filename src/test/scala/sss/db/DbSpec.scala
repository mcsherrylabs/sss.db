package sss.db

import org.scalatest.{BeforeAndAfterAll, SequentialNestedSuiteExecution, Suites}


class DbSpec extends
  Suites(
    new DbV1Spec,
    new DbV2Spec,
    new PagedViewSpec,
    new ForComprehensionSpec,
    new SqlInterpolatorSpec,
  ) with
  BeforeAndAfterAll with
  SequentialNestedSuiteExecution {

}

