#!/usr/bin/env bash

# run on DAQ unit after a new file chunk has been created.
# $1 is the full file name of the completed file.

exec > >(tee -i -a files/upload.log)

for i in {1..100}; do
  rsync -a --timeout=15 --remove-source-files -e "ssh -i $HOME/.ssh/energy-daq" "$1" $SERVER:/energy-daq/tmp/ && break || sleep 15
done

echo "Uploaded $1 successfully."
