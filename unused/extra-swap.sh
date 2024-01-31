 2000  df -lh
 2001  df -h
 2002  sudo fallocate -l 5G /swapfile_extend_1GB
 2003  df -h
 2004  sudo chmod 600 /swapfile_extend_1GB 
 2005  sudo mv /swapfile_extend_1GB  /swapfile_extend
 2006  sudo mkswap /swapfile_extend 
 2007  sudo swapon /swapfile_extend
 2008  history | tail
 2009  history | tail > ~/Documents/TSE/AppliedEconometrics/repo/unused/extra-swap.sh
