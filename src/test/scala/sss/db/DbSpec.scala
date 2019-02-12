package sss.db

import org.scalatest.{BeforeAndAfterAll, SequentialNestedSuiteExecution, Suites}


class DbSpec extends
  Suites(
      new DbV1Spec
  ) with
  BeforeAndAfterAll with
  SequentialNestedSuiteExecution {

}

