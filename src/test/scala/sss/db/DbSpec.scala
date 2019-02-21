package sss.db

import org.scalatest.{BeforeAndAfterAll, SequentialNestedSuiteExecution, Suites}


class DbSpec extends
  Suites(
      new DbV1Spec,
      new DbV2Spec
  ) with
  BeforeAndAfterAll with
  SequentialNestedSuiteExecution {

}

