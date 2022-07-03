# Labs Setup #

## Aws ##
* EC2 t2.large instances with Ubuntu 16.04 as InfluxDb requires <a href="https://docs.influxdata.com/influxdb/v1.3/introduction/installation/#hosting-on-aws" target="_blank">8GB of RAM</a>
* Root access via ssh using public/private key
* For Windows user please download <a href="http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html" target="_blank">putty
* For Mac and Linux users - ssh is available via terminal window
* There are also browser extensions: [Secure Shell for Chrome](https://chrome.google.com/webstore/detail/secure-shell/pnhechapfaindjhompbnflcldabbghjo?utm_source=chrome-ntp-icon) and [FireSSH for Firefox](http://firessh.net/)
* If you don't have private/public key pair available follow instructions for <a href="https://docs.joyent.com/public-cloud/getting-started/ssh-keys/generating-an-ssh-key-manually/manually-generating-your-ssh-key-in-windows" target="_blank"> Windows</a> or for <a href="https://docs.joyent.com/public-cloud/getting-started/ssh-keys/generating-an-ssh-key-manually/manually-generating-your-ssh-key-in-mac-os-x" target="_blank">Mac</a>
* Email public key to vladimir.khazin@icssolutions.ca to configure your access
* You can also use private ssh key emailed to you before the course
  * <a href="https://www.electrictoolbox.com/putty-rsa-dsa-keys/" target="_blank">How to import private key using Putty</a>
  * Setting permissions on Linux/MacOs: ```chmod 600 /path/to/private-key/file```
* Another option is to use <a href="https://chrome.google.com/webstore/detail/secure-shell/pnhechapfaindjhompbnflcldabbghjo?utm_source=chrome-ntp-icon" target="_blank">Secure Shell Chrome Extension to launch in-browser ssh</a>