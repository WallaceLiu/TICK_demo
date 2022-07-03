# disk-growth-alert

DEFINE: kapacitor define disk_growth_alert -type batch -tick projected-disk-full.tick -dbrp telegraf.autogen

ENABLE: kapacitor enable disk_grwoth_alert

The tick script is set up to look at the telegraf database, the autogen default retention policy, and from within that the disk measurement which is 
typically written to by the disk plugin of Telegraf.

The script is current setup to *only* look at a host called 'psuedo' which is the host tag associated with some synthentic data that can be 
written in by the shell scripts in this directory.

* base.sh writes in used=100 out of total=1000, ie 10% used and it writes this every 10 seconds. Having run this for a few minutes you can stop it and then run
* warn.sh which increnenets used from 100 by 2 every 10s. The tick script will determine that this will reach 1000 within 180 minutes which will generate a warn alarm. You can then stop this
script and run 
* crit.sh which will increment by 4 every 10s which will trigger a critial alert as will reach 1000 in less than 60 minutes.


