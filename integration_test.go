package imosql_test

import (
  imosql "./"
  "testing"
  "flag"
)

var enableIntegrationTest = flag.Bool(
  "enable_integration_test", false,
  "Enables integration test using an actual MySQL server.")


func TestCurrentTime(t *testing.T) {
  if !*enableIntegrationTest {
    return
  }
  _, _ = imosql.GetMysql("hoge")
  return
}
