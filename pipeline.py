from data_collector import local2bronze_firewall, local2bronze_ap

import time
from datetime import datetime

while True:
  local2bronze_ap.__init__("AP001")
  time.sleep(18)

  local2bronze_ap.__init__("AP002")
  time.sleep(18)

  local2bronze_ap.__init__("AP003")
  time.sleep(9)

  local2bronze_firewall.__init__()
  time.sleep(9)