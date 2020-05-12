#!/bin/sh
docker build --build-arg http_proxy=http://10.10.15.86:3128 --build-arg https_proxy=http://10.10.15.86:3128 . -t inventorysimulationflask && \
  docker tag inventorysimulationflask registry.fozzy.lan/calcengine/inventorysimulationflask && \
  docker push registry.fozzy.lan/calcengine/inventorysimulationflask